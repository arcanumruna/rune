// tests/detail/OneToOneRingBufferTest.cpp — unit tests for the faithful port of
// Aeron's OneToOneRingBuffer (F8a fix round 1, I3), mirroring M2ORingBufferTest:
// in-order drain, wrap padding, back-pressure, the producer head cache, the two
// single-writer closed words, a throwing fill, and a real two-thread SPSC run
// (TSan: the ring's own ordering is the only synchronisation).

#include <rune/client/detail/OneToOneRingBuffer.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

using rune::client::detail::OneToOneRingBuffer;

namespace {

constexpr int32_t     kMsgTypeId  = 7;
constexpr std::size_t kPayloadLen = 32;  // 8 B header + 32 B = 40 B record

struct Ring {
    std::unique_ptr<uint8_t, void (*)(void*)> buf;
    OneToOneRingBuffer ring;

    explicit Ring(int32_t usableCap)
        : buf(alloc(static_cast<std::size_t>(usableCap) + OneToOneRingBuffer::trailerLength()))
        , ring(buf.get(), static_cast<std::size_t>(usableCap) + OneToOneRingBuffer::trailerLength()) {}

    static std::unique_ptr<uint8_t, void (*)(void*)> alloc(std::size_t size) {
        void* raw = nullptr;
        if (::posix_memalign(&raw, 64, size) != 0 || !raw) throw std::bad_alloc{};
        std::memset(raw, 0, size);
        return {static_cast<uint8_t*>(raw), [](void* p) { ::free(p); }};
    }

    bool writeOne(uint8_t fill) {
        uint8_t src[kPayloadLen];
        std::memset(src, fill, sizeof src);
        aeron::concurrent::AtomicBuffer ab(src, sizeof src);
        return ring.write(kMsgTypeId, ab, 0, static_cast<aeron::util::index_t>(sizeof src));
    }

    int readAll(std::vector<uint8_t>& seen, int limit = INT32_MAX) {
        return ring.read(
            [&](int32_t msgTypeId, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                aeron::util::index_t len) {
                EXPECT_EQ(msgTypeId, kMsgTypeId);
                EXPECT_EQ(len, static_cast<aeron::util::index_t>(kPayloadLen));
                seen.push_back(*(b.buffer() + off));
            },
            limit);
    }
};

} // namespace

TEST(OneToOneRingBuffer, ReadDrainsAllMessagesInOrderNoWrap) {
    Ring r(512);
    for (uint8_t i = 0; i < 5; ++i) ASSERT_TRUE(r.writeOne(i));
    std::vector<uint8_t> seen;
    EXPECT_EQ(r.readAll(seen), 5);
    ASSERT_EQ(seen.size(), 5u);
    for (uint8_t i = 0; i < 5; ++i) EXPECT_EQ(seen[i], i);
    EXPECT_EQ(r.ring.size(), 0);
}

TEST(OneToOneRingBuffer, WriteFailsWhenFullAndSucceedsAfterDrain) {
    Ring r(512);
    int written = 0;
    while (r.writeOne(static_cast<uint8_t>(written))) ++written;
    EXPECT_EQ(written, 12);  // 12 * 40 = 480 <= 512 < 13 * 40
    std::vector<uint8_t> seen;
    EXPECT_EQ(r.readAll(seen, 1), 1);
    EXPECT_TRUE(r.writeOne(200)) << "draining one record reopens capacity";
}

// Aeron's head cache: a write that fits the cached view never needs the
// consumer's head; when it does not fit, the producer re-reads the head and
// refreshes the cache.
TEST(OneToOneRingBuffer, ProducerRefreshesItsHeadCacheOnlyWhenNeeded) {
    Ring r(512);
    for (int i = 0; i < 12; ++i) ASSERT_TRUE(r.writeOne(1));
    std::vector<uint8_t> seen;
    EXPECT_EQ(r.readAll(seen), 12);
    // Cache still says "full" (head 0); the write must re-read the real head.
    EXPECT_TRUE(r.writeOne(2));
    EXPECT_EQ(r.ring.consumerPosition(), 480);
}

// Mirrors M2ORingBuffer's wrap test: the wrapped record is preceded by padding.
// Faithful to Aeron, a read() that consumes only the padding returns 0 but has
// advanced the head; the next read() returns the wrapped record.
TEST(OneToOneRingBuffer, WrapPaddingIsSkippedAndTheWrappedRecordDelivered) {
    Ring r(512);
    for (uint8_t i = 0; i < 12; ++i) ASSERT_TRUE(r.writeOne(i));
    std::vector<uint8_t> seen;
    ASSERT_EQ(r.readAll(seen, 5), 5);
    ASSERT_TRUE(r.writeOne(99));  // 32 B to the end < 40 B: pads and wraps
    seen.clear();
    int total = 0;
    for (int i = 0; i < 3 && seen.size() < 8; ++i) total += r.readAll(seen);
    EXPECT_EQ(total, 8);
    ASSERT_EQ(seen.size(), 8u);
    EXPECT_EQ(seen.back(), 99);
    EXPECT_EQ(r.ring.size(), 0);
}

TEST(OneToOneRingBuffer, WrapMayFillUpToTheHeadExactlyThenRefuses) {
    Ring r(512);
    for (uint8_t i = 0; i < 12; ++i) ASSERT_TRUE(r.writeOne(i));
    std::vector<uint8_t> seen;
    ASSERT_EQ(r.readAll(seen, 1), 1);  // head at 40: exactly one record fits at index 0
    EXPECT_TRUE(r.writeOne(1));        // 32 B padding + 40 B record: ring now exactly full
    EXPECT_EQ(r.ring.size(), 512);
    EXPECT_FALSE(r.writeOne(2));
}

TEST(OneToOneRingBuffer, ClosedWordsAreIndependentSingleWriterAndSticky) {
    Ring r(512);
    EXPECT_FALSE(r.ring.closed());
    r.ring.consumerClose();
    EXPECT_TRUE(r.ring.consumerClosed());
    EXPECT_FALSE(r.ring.producerClosed());
    EXPECT_TRUE(r.ring.closed());
    r.ring.producerClose();
    r.ring.producerClose();
    EXPECT_TRUE(r.ring.producerClosed());
    EXPECT_TRUE(r.writeOne(1)) << "closed words are signals; write() itself is unaffected";
}

TEST(OneToOneRingBuffer, ThrowingFillPublishesNothing) {
    Ring r(512);
    EXPECT_THROW(r.ring.writeWith(kMsgTypeId, kPayloadLen,
                                  [](aeron::concurrent::AtomicBuffer&, aeron::util::index_t) {
                                      throw std::runtime_error("fill failed");
                                  }),
                 std::runtime_error);
    std::vector<uint8_t> seen;
    EXPECT_EQ(r.readAll(seen), 0);
    EXPECT_TRUE(r.writeOne(5));
    EXPECT_EQ(r.readAll(seen), 1);
    EXPECT_EQ(seen.at(0), 5);
}

TEST(OneToOneRingBuffer, OversizedMessageThrows) {
    Ring r(512);  // maxMsgLength = 64
    EXPECT_THROW(r.ring.writeWith(kMsgTypeId, 65,
                                  [](aeron::concurrent::AtomicBuffer&, aeron::util::index_t) {}),
                 aeron::util::IllegalArgumentException);
    EXPECT_FALSE(r.ring.unblock());
}

// Real SPSC run: every message arrives once, in order, with its payload
// intact.  The test's own flags are harness-only; the ring carries the data.
TEST(OneToOneRingBuffer, TwoThreadSpscDeliversEverythingInOrder) {
    Ring r(4096);
    constexpr uint32_t kN = 50'000;
    std::thread producer([&] {
        for (uint32_t i = 0; i < kN;) {
            const bool ok = r.ring.writeWith(
                kMsgTypeId, sizeof(uint32_t),
                [&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t at) {
                    std::memcpy(b.buffer() + at, &i, sizeof i);
                });
            if (ok) ++i; else std::this_thread::yield();
        }
        r.ring.producerClose();
    });
    uint32_t expected = 0;
    bool inOrder = true;
    while (expected < kN) {
        const int n = r.ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& b,
                                      aeron::util::index_t off, aeron::util::index_t) {
            uint32_t v;
            std::memcpy(&v, b.buffer() + off, sizeof v);
            if (v != expected) inOrder = false;
            ++expected;
        });
        if (n == 0) std::this_thread::yield();
    }
    producer.join();
    EXPECT_TRUE(inOrder);
    EXPECT_TRUE(r.ring.producerClosed());
    EXPECT_EQ(r.ring.size(), 0);
}
