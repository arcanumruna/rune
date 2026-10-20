// tests/detail/M2ORingBufferTest.cpp — direct unit tests for the ring
// primitive itself (fix2 Important-1): read()/controlledRead() must not
// report 0 work ("ring empty, nothing to do") when a call consumes only
// end-of-buffer wrap padding while a real message sits right past the wrap.

#include <rune/client/detail/M2ORingBuffer.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <thread>
#include <stdexcept>
#include <vector>

using rune::client::detail::ControlledAction;
using rune::client::detail::M2ORingBuffer;

namespace {

constexpr int32_t kMsgTypeId = 7;

// 8-byte header + 32-byte payload = 40-byte aligned record (already a
// multiple of 8, no intra-record padding). With a 512-byte usable ring, 12
// records (480 B) fit before the 13th hits real back-pressure; draining 5
// reopens total capacity but leaves only 32 B contiguous to the physical
// buffer end — too small for a 40-byte record — so the next write pads that
// remainder and wraps to offset 0.
constexpr std::size_t kPayloadLen = 32;

struct Ring {
    std::unique_ptr<uint8_t, void (*)(void*)> buf;
    M2ORingBuffer ring;

    explicit Ring(int32_t usableCap)
        : buf(alloc(static_cast<std::size_t>(usableCap) + M2ORingBuffer::trailerLength()))
        , ring(buf.get(), static_cast<std::size_t>(usableCap) + M2ORingBuffer::trailerLength())
    {}

    static std::unique_ptr<uint8_t, void (*)(void*)> alloc(std::size_t size) {
        void* raw = nullptr;
        if (::posix_memalign(&raw, 64, size) != 0 || !raw) throw std::bad_alloc{};
        std::memset(raw, 0, size);
        return {static_cast<uint8_t*>(raw), [](void* p) { ::free(p); }};
    }

    // Writes one kPayloadLen-byte message; returns true on success.
    bool writeOne(uint8_t fill) {
        const auto idx = ring.tryClaim(kMsgTypeId, kPayloadLen);
        if (idx < 0) return false;
        std::memset(ring.base() + idx, fill, kPayloadLen);
        ring.commit(idx);
        return true;
    }
};

} // namespace

// ---------------------------------------------------------------------------
// read() — the agent command ring's consumer-side drain primitive.
// ---------------------------------------------------------------------------

TEST(M2ORingBuffer, ReadDrainsAllMessagesInOrderNoWrap) {
    Ring r(512);
    for (uint8_t i = 0; i < 5; ++i) ASSERT_TRUE(r.writeOne(i));

    std::vector<uint8_t> seen;
    const int n = r.ring.read(
        [&](int32_t msgTypeId, aeron::concurrent::AtomicBuffer& buf,
            aeron::util::index_t offset, aeron::util::index_t /*len*/) {
            EXPECT_EQ(msgTypeId, kMsgTypeId);
            seen.push_back(*(buf.buffer() + offset));
        });
    EXPECT_EQ(n, 5);
    ASSERT_EQ(seen.size(), 5u);
    for (uint8_t i = 0; i < 5; ++i) EXPECT_EQ(seen[i], i);
}

// Regression (fix2 Important-1): a read() call that lands exactly on
// end-of-buffer padding must not report 0 while a real message sits right
// past the wrap — it must continue into it within the SAME call.
TEST(M2ORingBuffer, ReadContinuesPastWrapPaddingInTheSameCallRegression) {
    Ring r(512);
    for (uint8_t i = 0; i < 12; ++i) ASSERT_TRUE(r.writeOne(i)); // fills to 480/512

    // Drain exactly 5, freeing room for a wrap (see the kPayloadLen comment).
    int drained = 0;
    for (int i = 0; i < 5; ++i) {
        drained += r.ring.read(
            [&](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                aeron::util::index_t) {},
            /*messageCountLimit=*/1);
    }
    ASSERT_EQ(drained, 5);

    // Wraps: physical tail is 32 B from the buffer end (< 40 B/record).
    ASSERT_TRUE(r.writeOne(99));

    // Drain messages 5..11 (7 of them) then the wrapped message 12 — whose
    // read() call first consumes ONLY the padding. Before the fix, that call
    // returned 0 even though message 12 sat right past the wrap.
    int totalRead = 0;
    uint8_t lastSeen = 0;
    for (int i = 0; i < 8 && totalRead < 8; ++i) {
        bool sawOne = false;
        const int n = r.ring.read(
            [&](int32_t msgTypeId, aeron::concurrent::AtomicBuffer& buf,
                aeron::util::index_t offset, aeron::util::index_t) {
                EXPECT_EQ(msgTypeId, kMsgTypeId);
                lastSeen = *(buf.buffer() + offset);
                sawOne = true;
            },
            /*messageCountLimit=*/1);
        ASSERT_NE(n, 0) << "read() reported idle (0) with " << (8 - totalRead)
                        << " messages still undrained — the ring-wrap padding "
                           "false-idle regressed";
        ASSERT_TRUE(sawOne);
        totalRead += n;
    }
    EXPECT_EQ(totalRead, 8);
    EXPECT_EQ(lastSeen, 99) << "the wrapped message must be the last one delivered";
}

// ---------------------------------------------------------------------------
// controlledRead() — same continuation, exercised directly (ProducerImpl's
// drainRing() already covers this indirectly; this is the primitive-level
// proof).
// ---------------------------------------------------------------------------

TEST(M2ORingBuffer, ControlledReadContinuesPastWrapPaddingInTheSameCallRegression) {
    Ring r(512);
    for (uint8_t i = 0; i < 12; ++i) ASSERT_TRUE(r.writeOne(i));

    int drained = 0;
    for (int i = 0; i < 5; ++i) {
        drained += r.ring.controlledRead(
            [&](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                aeron::util::index_t) { return ControlledAction::CONTINUE; },
            /*messageCountLimit=*/1);
    }
    ASSERT_EQ(drained, 5);

    ASSERT_TRUE(r.writeOne(99));

    int totalRead = 0;
    uint8_t lastSeen = 0;
    for (int i = 0; i < 8 && totalRead < 8; ++i) {
        const int n = r.ring.controlledRead(
            [&](int32_t msgTypeId, aeron::concurrent::AtomicBuffer& buf,
                aeron::util::index_t offset, aeron::util::index_t) {
                EXPECT_EQ(msgTypeId, kMsgTypeId);
                lastSeen = *(buf.buffer() + offset);
                return ControlledAction::CONTINUE;
            },
            /*messageCountLimit=*/1);
        ASSERT_NE(n, 0) << "controlledRead() reported idle (0) with " << (8 - totalRead)
                        << " messages still undrained";
        totalRead += n;
    }
    EXPECT_EQ(totalRead, 8);
    EXPECT_EQ(lastSeen, 99);
}

// ---------------------------------------------------------------------------
// closed() — fix1 C2/I3's ring trailer word.
// ---------------------------------------------------------------------------

TEST(M2ORingBuffer, ClosedDefaultsFalseAndIsSticky) {
    Ring r(512);
    EXPECT_FALSE(r.ring.closed());
    r.ring.close();
    EXPECT_TRUE(r.ring.closed());
}

// ---------------------------------------------------------------------------
// fix5 Important-2(a): a throw after tryClaim() must never orphan the claim.
// An orphaned claim (length word left negative forever) blocks the single
// consumer at that record for good — every committed record behind it is
// unreachable. Aeron's claim contract: a successful claim is ALWAYS either
// committed or aborted.
// ---------------------------------------------------------------------------

namespace {
// Reads everything currently readable; returns the payload's first byte of
// each message, in order.
std::vector<uint8_t> readAll(M2ORingBuffer& ring) {
    std::vector<uint8_t> seen;
    for (int pass = 0; pass < 4; ++pass) {
        ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& buf,
                      aeron::util::index_t offset, aeron::util::index_t) {
            seen.push_back(*(buf.buffer() + offset));
        });
    }
    return seen;
}
} // namespace

TEST(M2ORingBuffer, WriteWithAbortsTheClaimWhenFillThrowsRegression) {
    Ring r(512);
    const auto tail0 = r.ring.tailPosition();
    EXPECT_THROW(r.ring.writeWith(kMsgTypeId, static_cast<aeron::util::index_t>(kPayloadLen),
                                  [](aeron::concurrent::AtomicBuffer&, aeron::util::index_t) {
                                      throw std::runtime_error("injected after claim");
                                  }),
                 std::runtime_error);
    EXPECT_GT(r.ring.tailPosition(), tail0) << "the claim really happened (space consumed)";

    ASSERT_TRUE(r.writeOne(9)); // committed, right behind the failed write
    const auto seen = readAll(r.ring);
    ASSERT_EQ(seen.size(), 1u) << "the aborted claim must be padding, and the consumer must "
                                  "proceed past it to the committed record";
    EXPECT_EQ(seen[0], 9);
    EXPECT_EQ(r.ring.headPosition(), r.ring.tailPosition());
}

// The same contract through write() itself: AtomicBuffer::putBytes's source
// bounds check throws (Debug builds; Aeron compiles bounds checks out of
// non-Debug configs via DISABLE_BOUNDS_CHECKS) AFTER the claim succeeded.
TEST(M2ORingBuffer, WriteAbortsTheClaimWhenPutBytesThrowsRegression) {
#if defined(DISABLE_BOUNDS_CHECKS)
    GTEST_SKIP() << "Aeron bounds checks compiled out in this config";
#else
    Ring r(512);
    uint8_t small[4] = {};
    aeron::concurrent::AtomicBuffer src(small, sizeof(small));
    // length 32 > the 4-byte source: src.boundsCheck throws inside putBytes.
    EXPECT_ANY_THROW(r.ring.write(kMsgTypeId, src, 0, static_cast<aeron::util::index_t>(kPayloadLen)));

    ASSERT_TRUE(r.writeOne(7));
    const auto seen = readAll(r.ring);
    ASSERT_EQ(seen.size(), 1u) << "pre-fix: the orphaned claim blocked the consumer forever";
    EXPECT_EQ(seen[0], 7);
    EXPECT_EQ(r.ring.headPosition(), r.ring.tailPosition());
#endif
}

// ---------------------------------------------------------------------------
// fix5 Important-2(b): unblock() — faithful port of Aeron's
// ManyToOneRingBuffer::unblock().
// ---------------------------------------------------------------------------

TEST(M2ORingBuffer, UnblockOnEmptyRingReturnsFalse) {
    Ring r(512);
    EXPECT_FALSE(r.ring.unblock());
    ASSERT_TRUE(r.writeOne(1));
    ASSERT_EQ(readAll(r.ring).size(), 1u);
    EXPECT_FALSE(r.ring.unblock()) << "head == tail after a full drain";
}

TEST(M2ORingBuffer, UnblockLeavesACommittedRecordAtHeadAlone) {
    Ring r(512);
    ASSERT_TRUE(r.writeOne(3));
    EXPECT_FALSE(r.ring.unblock()) << "length > 0 at head: nothing is blocked";
    const auto seen = readAll(r.ring);
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0], 3);
}

// 2b-B Task 4 fix round (M1): an unblock() landing BETWEEN commit()'s verify
// and its publish used to be overwritten by an equal store, so commit() returned
// success for a record that was already padding (silent loss).  The publish is a
// CAS now: the loser throws.  The interleaving is forced through commitWith().
TEST(M2ORingBuffer, CommitRacingUnblockBetweenVerifyAndPublishThrowsRegression) {
    Ring r(512);
    const auto claim = r.ring.tryClaim(kMsgTypeId, kPayloadLen);
    ASSERT_GE(claim, 0);
    bool unblocked = false;
    EXPECT_THROW(r.ring.commitWith(claim, [&] { unblocked = r.ring.unblock(); }), std::exception)
        << "the record was turned into padding under the writer: commit must not report success";
    EXPECT_TRUE(unblocked);
    EXPECT_TRUE(readAll(r.ring).empty()) << "padding only: nothing was published";
    EXPECT_EQ(r.ring.headPosition(), r.ring.tailPosition());
    // And the uncontended commit still publishes.
    const auto c2 = r.ring.tryClaim(kMsgTypeId, kPayloadLen);
    ASSERT_GE(c2, 0);
    EXPECT_NO_THROW(r.ring.commitWith(c2, [] {}));
}

// Claimed (header written, length negative) but never committed.
TEST(M2ORingBuffer, UnblockTurnsAnUncommittedClaimAtHeadIntoPadding) {
    Ring r(512);
    const auto orphan = r.ring.tryClaim(kMsgTypeId, kPayloadLen);
    ASSERT_GE(orphan, 0);
    ASSERT_TRUE(r.writeOne(5));
    ASSERT_TRUE(r.writeOne(6));
    EXPECT_TRUE(readAll(r.ring).empty()) << "blocked behind the uncommitted claim";

    EXPECT_TRUE(r.ring.unblock());
    const auto seen = readAll(r.ring);
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0], 5);
    EXPECT_EQ(seen[1], 6);
    EXPECT_EQ(r.ring.headPosition(), r.ring.tailPosition());
    EXPECT_THROW(r.ring.commit(orphan), aeron::util::IllegalStateException)
        << "a late commit of an unblocked claim must be refused, never published";
}

// Tail CAS'd but the header never written (length word still 0): unblock
// scans forward to the next non-zero length word and pads up to it.
TEST(M2ORingBuffer, UnblockPadsAHeaderlessClaimUpToTheNextRecord) {
    Ring r(512);
    const auto orphan = r.ring.tryClaim(kMsgTypeId, kPayloadLen);
    ASSERT_GE(orphan, 0);
    std::memset(r.ring.base() + orphan - 8, 0, 8); // simulate: header not yet written
    ASSERT_TRUE(r.writeOne(8));
    EXPECT_TRUE(readAll(r.ring).empty());

    EXPECT_TRUE(r.ring.unblock());
    const auto seen = readAll(r.ring);
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0], 8);
    EXPECT_EQ(r.ring.headPosition(), r.ring.tailPosition());
}

// A headerless claim with NOTHING written behind it: the scan finds no
// non-zero length word before the producer index, so there is nothing to pad
// up to — Aeron returns false (and so must we).
TEST(M2ORingBuffer, UnblockHeaderlessClaimWithNothingBehindReturnsFalse) {
    Ring r(512);
    const auto orphan = r.ring.tryClaim(kMsgTypeId, kPayloadLen);
    ASSERT_GE(orphan, 0);
    std::memset(r.ring.base() + orphan - 8, 0, 8);
    EXPECT_FALSE(r.ring.unblock());
    EXPECT_NE(r.ring.headPosition(), r.ring.tailPosition());
}

// ---------------------------------------------------------------------------
// F8c C1(a): linearizable close — the closed flag is the top bit of the tail
// word that tryClaim() already CASes, so every claim is totally ordered with
// the close: a claim that won its CAS before the close is drained (even if it
// commits afterwards), a claim after it fails with kClaimClosed.
// ---------------------------------------------------------------------------

// Deterministic single-thread contract.
TEST(M2ORingBuffer, M2OClaimAfterCloseIsRejectedRegression) {
    Ring r(512);
    ASSERT_TRUE(r.writeOne(1));
    const auto pending = r.ring.tryClaim(kMsgTypeId, kPayloadLen);  // claimed, not committed
    ASSERT_GE(pending, 0);
    const std::int64_t tailBefore = r.ring.tailPosition();
    EXPECT_TRUE(r.ring.close()) << "the first close() closes";
    EXPECT_FALSE(r.ring.close()) << "close() is idempotent";
    EXPECT_TRUE(r.ring.closed());
    EXPECT_EQ(r.ring.tailPosition(), tailBefore) << "the closed bit is not part of the position";
    EXPECT_EQ(r.ring.tryClaim(kMsgTypeId, kPayloadLen), M2ORingBuffer::kClaimClosed);
    EXPECT_FALSE(r.writeOne(3)) << "write() after close must be rejected";
    std::memset(r.ring.base() + pending, 2, kPayloadLen);
    r.ring.commit(pending);  // the pre-close claim commits AFTER the close
    EXPECT_EQ(readAll(r.ring), (std::vector<uint8_t>{1, 2}))
        << "a claim made before the close is drained";
    EXPECT_EQ(r.ring.headPosition(), r.ring.tailPosition()) << "fully drained";
}

// N writers vs a closer (the consumer), 1000 iterations (run under TSan too):
// every accepted claim is drained, in per-writer order with no gap, and every
// claim attempted after the close is rejected.  The close point varies per
// iteration; no sleeps — every wait is on another thread's progress.
TEST(M2ORingBuffer, M2OCloseLinearizesWithClaimsRegression) {
    constexpr int kWriters = 4;
    constexpr int kIterations = 1000;
    struct Rec { uint32_t writer; uint32_t seq; };
    for (int it = 0; it < kIterations; ++it) {
        Ring r(4096);
        uint32_t accepted[kWriters] = {};
        bool     sawClosed[kWriters] = {};
        std::vector<std::thread> ws;
        for (int w = 0; w < kWriters; ++w) {
            ws.emplace_back([&r, &accepted, &sawClosed, w] {
                for (;;) {
                    const auto idx = r.ring.tryClaim(kMsgTypeId, sizeof(Rec));
                    if (idx == M2ORingBuffer::kClaimClosed) { sawClosed[w] = true; return; }
                    if (idx < 0) continue;  // full: the consumer is draining
                    const Rec rec{static_cast<uint32_t>(w), accepted[w]};
                    std::memcpy(r.ring.base() + idx, &rec, sizeof rec);
                    r.ring.commit(idx);
                    ++accepted[w];
                }
            });
        }
        uint32_t seen[kWriters] = {};
        bool inOrder = true;
        auto drain = [&] {
            return r.ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                                   aeron::util::index_t) {
                Rec rec;
                std::memcpy(&rec, b.buffer() + off, sizeof rec);
                if (rec.writer >= kWriters || rec.seq != seen[rec.writer]) inOrder = false;
                else ++seen[rec.writer];
            });
        };
        const int closeAfter = (it * 37) % 400;  // vary the close point
        int consumed = 0;
        while (consumed < closeAfter) consumed += drain();
        ASSERT_TRUE(r.ring.close());
        // The close froze the tail: no claim can move it any more.  Close drain:
        // every claim that won its CAS before the close commits (its writer is
        // live), so the head reaches that frozen tail.
        const std::int64_t frozenTail = r.ring.tailPosition();
        while (r.ring.headPosition() < frozenTail) drain();
        for (auto& t : ws) t.join();
        ASSERT_EQ(r.ring.tailPosition(), frozenTail)
            << "iteration " << it << ": a claim succeeded after close()";
        ASSERT_TRUE(inOrder) << "iteration " << it;
        for (int w = 0; w < kWriters; ++w) {
            ASSERT_TRUE(sawClosed[w]) << "iteration " << it << " writer " << w;
            ASSERT_EQ(seen[w], accepted[w])
                << "iteration " << it << " writer " << w
                << ": an accepted claim was not drained (claim after close?)";
        }
        ASSERT_EQ(r.ring.tryClaim(kMsgTypeId, sizeof(Rec)), M2ORingBuffer::kClaimClosed);
    }
}

// F8c C1(a) fix round 1: a CLOSED ring that is also FULL reports kClaimClosed,
// never INSUFFICIENT_CAPACITY (-2) — a writer that retries on -2 (back-pressure)
// would otherwise spin forever on a ring that will never drain for it.
TEST(M2ORingBuffer, M2OClosedFullRingReturnsClaimClosedRegression) {
    Ring r(512);
    int n = 0;
    while (r.writeOne(static_cast<uint8_t>(n))) ++n;
    ASSERT_EQ(n, 12);
    ASSERT_EQ(r.ring.tryClaim(kMsgTypeId, kPayloadLen), -2) << "full and open: back-pressure";
    ASSERT_TRUE(r.ring.close());
    EXPECT_EQ(r.ring.tryClaim(kMsgTypeId, kPayloadLen), M2ORingBuffer::kClaimClosed)
        << "full and closed: Closed, not back-pressure";
    EXPECT_EQ(readAll(r.ring).size(), 12u) << "pre-close records still drain";
    EXPECT_EQ(r.ring.tryClaim(kMsgTypeId, kPayloadLen), M2ORingBuffer::kClaimClosed)
        << "drained and closed: Closed";
}
