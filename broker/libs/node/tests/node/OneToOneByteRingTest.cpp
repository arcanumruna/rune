// ---------------------------------------------------------------------------
// OneToOneByteRingTest.cpp — unit tests for rune::node::OneToOneByteRing.
//
// Test matrix:
//   1. WriteAndDrain — write N frames of varying lengths + msgTypeIds;
//      drain; assert byte-identical payloads and correct msgTypeIds;
//      assert correct drained count.
//   2. DrainWithLimit — drain respects the per-call message limit.
//   3. FullRingReturnsFalse — writing beyond ring capacity returns false
//      (back-pressure, no ack/drop).
//   4. EmptyRingDrainsZero — drain on an empty ring returns 0.
// ---------------------------------------------------------------------------

#include <rune/node/OneToOneByteRing.h>

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <cstdint>
#include <cstring>

using rune::node::OneToOneByteRing;

namespace {

// Helper: build a simple payload of `len` bytes using a repeating pattern.
std::vector<uint8_t> makePayload(std::size_t len, uint8_t seed = 0xAB) {
    std::vector<uint8_t> buf(len);
    for (std::size_t i = 0; i < len; ++i) {
        buf[i] = static_cast<uint8_t>((seed + i) & 0xFF);
    }
    return buf;
}

} // namespace

// ---------------------------------------------------------------------------
// 1. WriteAndDrain — varying lengths + msgTypeIds
// ---------------------------------------------------------------------------
TEST(OneToOneByteRing, WriteAndDrain) {
    OneToOneByteRing ring(/*payloadCapacity=*/4096);

    struct Frame {
        std::vector<uint8_t> payload;
        int32_t              msgTypeId;
    };

    std::vector<Frame> frames = {
        { makePayload(1,   0x01), 1 },
        { makePayload(64,  0x02), 2 },
        { makePayload(128, 0x03), 3 },
        { makePayload(17,  0x04), 5 },   // msgTypeId = 5 (non-contiguous)
        { makePayload(256, 0x05), 7 },   // msgTypeId = 7
    };

    for (auto& f : frames) {
        ASSERT_TRUE(ring.write(f.payload.data(), f.payload.size(), f.msgTypeId))
            << "write() must succeed when ring is not full";
    }

    std::size_t index = 0;
    int count = ring.drain(
        [&](int32_t msgTypeId,
            aeron::concurrent::AtomicBuffer& buf,
            aeron::util::index_t             offset,
            aeron::util::index_t             length)
        {
            ASSERT_LT(index, frames.size()) << "more messages than written";
            const auto& expected = frames[index];

            EXPECT_EQ(msgTypeId, expected.msgTypeId)
                << "msgTypeId mismatch at frame " << index;
            ASSERT_EQ(static_cast<std::size_t>(length), expected.payload.size())
                << "length mismatch at frame " << index;

            // Byte-identical payload check
            EXPECT_EQ(std::memcmp(buf.buffer() + offset,
                                  expected.payload.data(),
                                  expected.payload.size()), 0)
                << "payload mismatch at frame " << index;
            ++index;
        },
        /*limit=*/static_cast<int>(frames.size() + 5));

    EXPECT_EQ(count, static_cast<int>(frames.size()));
    EXPECT_EQ(index, frames.size());
}

// ---------------------------------------------------------------------------
// 2. DrainWithLimit — drain(N) must respect the per-call limit
// ---------------------------------------------------------------------------
TEST(OneToOneByteRing, DrainWithLimit) {
    OneToOneByteRing ring(4096);

    auto payload = makePayload(32, 0xCC);
    for (int i = 0; i < 6; ++i) {
        ASSERT_TRUE(ring.write(payload.data(), payload.size(), /*msgTypeId=*/1));
    }

    int first = ring.drain([](int32_t, aeron::concurrent::AtomicBuffer&,
                               aeron::util::index_t, aeron::util::index_t) {},
                           /*limit=*/3);
    EXPECT_EQ(first, 3);

    int second = ring.drain([](int32_t, aeron::concurrent::AtomicBuffer&,
                                aeron::util::index_t, aeron::util::index_t) {},
                            /*limit=*/10);
    EXPECT_EQ(second, 3);  // remaining 3

    int third = ring.drain([](int32_t, aeron::concurrent::AtomicBuffer&,
                               aeron::util::index_t, aeron::util::index_t) {},
                           /*limit=*/10);
    EXPECT_EQ(third, 0);   // empty
}

// ---------------------------------------------------------------------------
// 3. FullRingReturnsFalse — back-pressure: write() returns false when full
// ---------------------------------------------------------------------------
TEST(OneToOneByteRing, FullRingReturnsFalse) {
    // Use a small ring so we can fill it quickly.
    // Minimum agrona capacity is 256 bytes; each slot needs HEADER(8) + payload aligned.
    // Use 512 payload bytes to keep it small but valid.
    OneToOneByteRing ring(512);

    // maxMsgLength = capacity / 8 = 64 bytes
    const aeron::util::index_t maxMsg = ring.maxMsgLength();
    ASSERT_GT(maxMsg, 0);

    auto payload = makePayload(static_cast<std::size_t>(maxMsg), 0xBB);

    // Write until full — at most a few frames fit in 512 bytes
    int written = 0;
    for (int i = 0; i < 100; ++i) {
        if (!ring.write(payload.data(), payload.size(), /*msgTypeId=*/1)) {
            break;
        }
        ++written;
    }
    EXPECT_GT(written, 0) << "Should have written at least one frame";

    // Next write MUST fail — ring is full (back-pressure)
    EXPECT_FALSE(ring.write(payload.data(), payload.size(), /*msgTypeId=*/1))
        << "write() must return false when ring is full";
}

// ---------------------------------------------------------------------------
// 4. EmptyRingDrainsZero
// ---------------------------------------------------------------------------
TEST(OneToOneByteRing, EmptyRingDrainsZero) {
    OneToOneByteRing ring(4096);
    int count = ring.drain([](int32_t, aeron::concurrent::AtomicBuffer&,
                               aeron::util::index_t, aeron::util::index_t) {},
                           /*limit=*/16);
    EXPECT_EQ(count, 0);
}
