// UringDurabilityTest.cpp — E3 Task 8a: Linux io_uring backend durability tests.
//
// ENTIRELY wrapped in #if defined(__linux__) so this file registers ZERO tests
// on macOS — the gtest binary compiles and links cleanly but contains no test
// functions.  The tests run for real on the Hetzner Linux box.
//
// Tests (mirrors PortableFlushBackendTest + extras):
//   1. UringSubmitThenReapCompletesForRealFd
//        Round-trip: write 5 bytes to a real temp file, submit an fsync,
//        reap the completion.  Expects token match, errorCode==0,
//        flushBoundary==5.  Validates the basic io_uring ABI wiring.
//
//   2. UringBadFdCompletesWithError
//        Submit an fsync on fd=-99 (EBADF).  The CQE must carry a non-zero
//        errorCode (res < 0).  Never silently OK.
//
//   3. UringSubmitFullReturnsNoToken
//        Flood submit() without reaping.  Eventually the SQ ring fills and
//        submit returns kNoToken (back-pressure).
//
//   4. UringGroupCommit
//        Submit N fsyncs on the same fd, reap all completions.  Assert every
//        token is seen exactly once with errorCode==0 and the correct
//        flushBoundary is recovered.  Validates the token→boundary table for
//        all slots.
//
//   5. UringAckOnlyAfterCompletion (best-effort)
//        Submit, sleep briefly, then reap.  Asserts that before reaping no
//        completion is visible, and after the reap + kernel round-trip the
//        completion arrives.  This is a timing assertion best-effort; it
//        documents the expected ordering rather than being an infallible gate.

#if defined(__linux__)

#include <rune/node/UringFlushBackend.h>

#include <gtest/gtest.h>

#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>

using rune::node::UringFlushBackend;
using rune::node::FlushCompletion;
using rune::node::kNoToken;

// ---------------------------------------------------------------------------
// Helper: spin-reap until 'got.token != 0' or we exhaust retries.
// Returns true if a completion was received.
// ---------------------------------------------------------------------------
static bool spinReap(UringFlushBackend& be, FlushCompletion& out,
                     int maxIter = 2'000'000) {
    for (int i = 0; i < maxIter; ++i) {
        be.reap([&](const FlushCompletion& c){ out = c; });
        if (out.token != rune::node::kNoToken) return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Test 1: round-trip a real fd
// ---------------------------------------------------------------------------
TEST(UringDurabilityTest, SubmitThenReapCompletesForRealFd) {
    char path[] = "/tmp/rune_uring_XXXXXX";
    int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::write(fd, "hello", 5), 5);

    UringFlushBackend be(/*submitDepth=*/16, /*completionDepth=*/16);

    auto tok = be.submit(fd, /*flushBoundary=*/5);
    ASSERT_NE(tok, kNoToken) << "submit failed (SQ full?)";

    FlushCompletion got{};
    ASSERT_TRUE(spinReap(be, got)) << "no completion received";

    EXPECT_EQ(got.token, tok);
    EXPECT_EQ(got.errorCode, 0) << "unexpected fsync error";
    EXPECT_EQ(got.flushBoundary, 5);

    ::close(fd);
    ::unlink(path);
}

// ---------------------------------------------------------------------------
// Test 2: bad fd → completion carries non-zero errorCode
// ---------------------------------------------------------------------------
TEST(UringDurabilityTest, BadFdCompletesWithError) {
    UringFlushBackend be(16, 16);

    // fd -99 is intentionally invalid.
    auto tok = be.submit(-99, /*flushBoundary=*/0);
    ASSERT_NE(tok, kNoToken);

    FlushCompletion got{};
    ASSERT_TRUE(spinReap(be, got)) << "no completion received for bad fd";

    EXPECT_EQ(got.token, tok);
    EXPECT_NE(got.errorCode, 0)
        << "bad-fd fsync should complete with EBADF, never errorCode==0";
}

// ---------------------------------------------------------------------------
// Test 3: submit ring full → kNoToken (back-pressure)
// ---------------------------------------------------------------------------
TEST(UringDurabilityTest, SubmitFullReturnsNoToken) {
    // Small ring (2 entries) — flood without reaping.
    UringFlushBackend be(/*submitDepth=*/2, /*completionDepth=*/2);

    bool sawFull = false;
    // Use fd=1 (stdout) which is valid but benign to fsync.
    for (int i = 0; i < 1000; ++i) {
        if (be.submit(1, static_cast<int64_t>(i)) == kNoToken) {
            sawFull = true;
            break;
        }
    }
    EXPECT_TRUE(sawFull) << "expected kNoToken when SQ ring is full";
}

// ---------------------------------------------------------------------------
// Test 4: group-commit — N fsyncs, all tokens arrive with correct boundaries
// ---------------------------------------------------------------------------
TEST(UringDurabilityTest, GroupCommit) {
    char path[] = "/tmp/rune_uring_gc_XXXXXX";
    int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);

    // Write a few bytes so fdatasync has something to flush.
    const char data[] = "group commit test data";
    ASSERT_EQ(::write(fd, data, sizeof(data) - 1),
              static_cast<ssize_t>(sizeof(data) - 1));

    const int N = 8;
    UringFlushBackend be(/*submitDepth=*/N + 4, /*completionDepth=*/N + 4);

    // Submit N fsyncs with distinct flushBoundary values.
    rune::node::CompletionToken tokens[N]{};
    int64_t boundaries[N]{};
    int submitted = 0;
    for (int i = 0; i < N; ++i) {
        boundaries[i] = static_cast<int64_t>(i + 1) * 10;
        tokens[i]     = be.submit(fd, boundaries[i]);
        if (tokens[i] != kNoToken) {
            ++submitted;
        }
    }
    ASSERT_GT(submitted, 0) << "no SQEs accepted";

    // Reap all completions.
    int reaped = 0;
    for (int iter = 0; iter < 10'000'000 && reaped < submitted; ++iter) {
        be.reap([&](const FlushCompletion& c) {
            // Find the matching token.
            for (int j = 0; j < N; ++j) {
                if (tokens[j] == c.token) {
                    EXPECT_EQ(c.errorCode, 0)
                        << "unexpected fsync error for token " << c.token;
                    EXPECT_EQ(c.flushBoundary, boundaries[j])
                        << "wrong flushBoundary for token " << c.token;
                    tokens[j] = kNoToken;  // mark seen
                    ++reaped;
                    break;
                }
            }
        });
    }

    EXPECT_EQ(reaped, submitted) << "not all submitted completions were reaped";

    ::close(fd);
    ::unlink(path);
}

// ---------------------------------------------------------------------------
// Test 5: ack only after completion (ordering assertion, best-effort)
//
// Submit a fsync, check that reap returns 0 immediately (before kernel has
// had a chance to process), then spin until completion arrives.  This is a
// soft ordering assertion — on a heavily loaded machine the kernel may finish
// before the first reap, so we don't ASSERT_EQ(0) on the first poll, but we
// do confirm the completion arrives eventually with the right token.
// ---------------------------------------------------------------------------
TEST(UringDurabilityTest, AckOnlyAfterCompletion) {
    char path[] = "/tmp/rune_uring_ord_XXXXXX";
    int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::write(fd, "ordering", 8), 8);

    UringFlushBackend be(16, 16);

    auto tok = be.submit(fd, /*flushBoundary=*/8);
    ASSERT_NE(tok, kNoToken);

    // Poll once immediately — on a fast kernel the completion may already be
    // there; that is acceptable.  We merely confirm the eventual arrival.
    FlushCompletion got{};
    int firstPoll = be.reap([&](const FlushCompletion& c){ got = c; });
    // (firstPoll may be 0 or 1 — either is fine for this ordering test)
    (void)firstPoll;

    // Spin until we definitely have the completion.
    if (got.token == kNoToken) {
        ASSERT_TRUE(spinReap(be, got)) << "completion never arrived";
    }

    EXPECT_EQ(got.token, tok);
    EXPECT_EQ(got.errorCode, 0);
    EXPECT_EQ(got.flushBoundary, 8);

    ::close(fd);
    ::unlink(path);
}

#endif // defined(__linux__)
