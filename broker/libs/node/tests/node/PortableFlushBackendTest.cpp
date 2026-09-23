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
