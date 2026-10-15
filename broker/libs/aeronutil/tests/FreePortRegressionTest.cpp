// FreePortRegressionTest.cpp — 2026-10-15 (regression-speed): the test UDP port allocator is collision-proof
// across concurrently running test processes, which is what lets the real-BrokerNode suites drop their
// `RESOURCE_LOCK rune_udp_ports` and run in parallel.  Before: a pid-scattered walk + bind probe — two
// processes could both see a port free before either bound it ("Address already in use" under ctest -j).
#include <rune/aeronutil/testing/FreePort.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <set>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

namespace {

namespace fp = rune::aeronutil::testing;

constexpr int kChildren = 8;
constexpr int kSingles  = 40;
constexpr int kRanges   = 6;
constexpr int kWidth    = 4;

/// Fork `kChildren` processes that allocate AT THE SAME TIME (they start together on a pipe barrier and stay
/// alive until every one has allocated, so no block is released mid-test); returns every child's ports.
std::vector<std::vector<uint16_t>> allocateConcurrently() {
    int go[2], out[2];
    if (::pipe(go) != 0 || ::pipe(out) != 0) return {};
    std::vector<pid_t> kids;
    for (int k = 0; k < kChildren; ++k) {
        const pid_t pid = ::fork();
        if (pid == 0) {
            ::close(go[1]);
            ::close(out[0]);
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
            (void)::read(go[0], &c, 1);   // hold the blocks until the parent read everyone (EOF on exit)
            ::_exit(0);
        }
        kids.push_back(pid);
    }
    ::close(go[0]);
    ::close(out[1]);
    const char start[kChildren] = {};
    (void)::write(go[1], start, kChildren);
    std::vector<std::vector<uint16_t>> all;
    for (int k = 0; k < kChildren; ++k) {
        uint32_t n = 0;
        if (::read(out[0], &n, sizeof n) != static_cast<ssize_t>(sizeof n)) break;
        std::vector<uint16_t> v(n);
        std::size_t got = 0;
        while (got < n * sizeof(uint16_t)) {
            const ssize_t r = ::read(out[0], reinterpret_cast<char*>(v.data()) + got, n * sizeof(uint16_t) - got);
            if (r <= 0) break;
            got += static_cast<std::size_t>(r);
        }
        all.push_back(std::move(v));
    }
    ::close(go[1]);   // release the children
    ::close(out[0]);
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
    for (int i = 0; i < 50; ++i) {
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
