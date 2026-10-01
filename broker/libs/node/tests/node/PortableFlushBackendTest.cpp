// PortableFlushBackendTest.cpp — E3 Task 2: unit tests for PortableFlushBackend.
//
// Three tests:
//   1. SubmitThenReapCompletesForRealFd  — round-trip with a real temp file.
//   2. BadFdCompletesWithError           — bad fd surfaces EBADF, never OK.
//   3. SubmitRingFullReturnsNoToken      — back-pressure: small ring floods → kNoToken.

#include <rune/node/PortableFlushBackend.h>

#include <gtest/gtest.h>

#include <cerrno>

#if defined(_WIN32)
#include <io.h>
#include <fcntl.h>
#else
#include <unistd.h>
#endif

// ---------------------------------------------------------------------------
// Test 1: round-trip a real fd
// ---------------------------------------------------------------------------
TEST(PortableFlushBackend, SubmitThenReapCompletesForRealFd) {
    char path[] = "/tmp/rune_pfb_XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::write(fd, "hello", 5), 5);

    rune::node::PortableFlushBackend be(/*submitDepth=*/16, /*completionDepth=*/16);
    auto tok = be.submit(fd, /*flushBoundary=*/5);
    ASSERT_NE(tok, rune::node::kNoToken);

    rune::node::FlushCompletion got{};
    for (int i = 0; i < 1'000'000 && got.token == 0; ++i) {
        be.reap([&](const rune::node::FlushCompletion& c){ got = c; });
    }

    EXPECT_EQ(got.token, tok);
    EXPECT_EQ(got.errorCode, 0);
    EXPECT_EQ(got.flushBoundary, 5);

    ::close(fd);
    ::unlink(path);
}

// ---------------------------------------------------------------------------
// Test 2: bad fd → completion carries an error, never a silent OK
// ---------------------------------------------------------------------------
TEST(PortableFlushBackend, BadFdCompletesWithError) {
    rune::node::PortableFlushBackend be(16, 16);
    auto tok = be.submit(/*fd=*/-99, 0);
    ASSERT_NE(tok, rune::node::kNoToken);

    rune::node::FlushCompletion got{};
    for (int i = 0; i < 1'000'000 && got.token == 0; ++i) {
        be.reap([&](const rune::node::FlushCompletion& c){ got = c; });
    }

    EXPECT_EQ(got.token, tok);
    EXPECT_NE(got.errorCode, 0);   // EBADF surfaced, never a silent OK
}

// ---------------------------------------------------------------------------
// Test 3: submit ring full → kNoToken (back-pressure)
// ---------------------------------------------------------------------------
TEST(PortableFlushBackend, SubmitRingFullReturnsNoToken) {
    rune::node::PortableFlushBackend be(/*submitDepth=*/2, /*completionDepth=*/2);
    // Flood without reaping; at least one submit must report back-pressure.
    bool sawFull = false;
    for (int i = 0; i < 1000; ++i) {
        if (be.submit(1, i) == rune::node::kNoToken) {
            sawFull = true;
            break;
        }
    }
    EXPECT_TRUE(sawFull);
}

// ---------------------------------------------------------------------------
// F9a regression: the TEARDOWN drain (requests still in the submit ring when
// the dtor's Stop arrives) must sync the SAME fd the request carries.  It used
// to reinterpret the SBE-encoded slot as the old POD FlushRequest, reading the
// SBE message header as the fd — syncing a garbage (possibly unrelated, live)
// fd and never the requested one.
//
// Determinism: the first sync blocks (test gate) while 40 more requests queue
// up; the flusher's drain pass handles at most 16, so once the gate opens (after
// the dtor posted Stop) the remaining ones are synced by the teardown path.
// Test-side sync state is test harness code (atomics allowed in tests).
// ---------------------------------------------------------------------------
#include <array>
#include <atomic>
#include <chrono>
#include <thread>

namespace {
constexpr int kTeardownReqs = 41;
std::array<int, 128>  g_syncedFds{};
int                   g_syncedCount = 0;           // flusher-thread only; read after join
std::atomic<bool>     g_firstSyncEntered{false};
std::atomic<bool>     g_gateOpen{false};

int recordingSync(int fd) {
    if (g_syncedCount == 0) {
        g_firstSyncEntered.store(true);
        while (!g_gateOpen.load()) std::this_thread::sleep_for(std::chrono::microseconds{50});
    }
    if (g_syncedCount < static_cast<int>(g_syncedFds.size())) g_syncedFds[g_syncedCount] = fd;
    ++g_syncedCount;
    return 0;
}
} // namespace

TEST(PortableFlushBackendRegression, TeardownDrainSyncsTheRequestedFd) {
    g_syncedCount = 0;
    g_firstSyncEntered = false;
    g_gateOpen = false;

    // Compile-time seam: the recording Sync is a template argument.
    auto* be = new rune::node::BasicPortableFlushBackend<&recordingSync>(/*submitDepth=*/64,
                                                                         /*completionDepth=*/64);
    ASSERT_NE(be->submit(1000, 1), rune::node::kNoToken);
    while (!g_firstSyncEntered.load()) std::this_thread::sleep_for(std::chrono::microseconds{50});
    for (int i = 1; i < kTeardownReqs; ++i) {
        ASSERT_NE(be->submit(1000 + i, 1 + i), rune::node::kNoToken);
    }
    std::thread destroyer([be] { delete be; });          // posts Stop, then joins
    std::this_thread::sleep_for(std::chrono::milliseconds{200});   // let Stop land first
    g_gateOpen = true;
    destroyer.join();                                     // join = happens-before

    ASSERT_EQ(g_syncedCount, kTeardownReqs) << "every queued request synced exactly once";
    for (int i = 0; i < kTeardownReqs; ++i) {
        EXPECT_EQ(g_syncedFds[static_cast<std::size_t>(i)], 1000 + i)
            << "request " << i << " synced the wrong fd";
    }
}
