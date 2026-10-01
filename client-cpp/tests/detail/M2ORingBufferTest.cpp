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
    r.ring.closed(true);
    EXPECT_TRUE(r.ring.closed());
}
