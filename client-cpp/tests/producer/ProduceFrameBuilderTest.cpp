// tests/producer/ProduceFrameBuilderTest.cpp — backlog B1/B4: the in-place
// Produce frame builder must emit EXACTLY the bytes of the protocol encoders
// (Produce::encode + RecordBatchBuilder::build) once sealed, for random record
// sets (empty / binary keys, 0..20 KiB values, timestamps before and after the
// base, varint width boundaries), and seal() must leave the CRC unset.
#include <rune/client/producer/ProduceFrameBuilder.h>
#include <rune/client/transport/FrameCrc.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

using rune::client::producer::ProduceFrameBuilder;
namespace rpc = rune::protocol::client;
namespace rb  = rune::protocol::batch;

namespace {

rpc::Produce header(std::mt19937_64& rng) {
    rpc::Produce m;
    m.sessionId      = static_cast<int64_t>(rng());
    m.correlationId  = static_cast<int64_t>(rng());
    m.topicId        = static_cast<int32_t>(rng());
    m.partition      = static_cast<int32_t>(rng() % 1000);
    m.partitionKnown = 1;
    m.acks           = static_cast<uint8_t>(rng() % 2);
    m.producerId     = static_cast<int64_t>(rng());
    m.producerEpoch  = static_cast<int16_t>(rng());
    m.baseSequence   = static_cast<int32_t>(rng());
    return m;
}

std::string randomBytes(std::mt19937_64& rng, std::size_t n) {
    std::string s(n, '\0');
    for (auto& c : s) c = static_cast<char>(rng());
    return s;
}

}  // namespace

TEST(ProduceFrameBuilder, SealedFrameIsByteIdenticalToTheProtocolEncoders) {
    std::mt19937_64 rng(0xB1B4);
    const std::size_t sizes[] = {0, 1, 63, 64, 127, 128, 8191, 8192, 16383, 16384, 20000};
    for (int iter = 0; iter < 300; ++iter) {
        const int64_t base = static_cast<int64_t>(rng() >> 2);
        ProduceFrameBuilder fb(0, base);
        rune::protocol::RecordBatchBuilder ref(0, base);
        if (iter % 7 == 0) {  // reuse after reset (capacity kept) must be identical too
            fb.append("warm", "up", base + 1);
            fb.reset(0, base);
        }
        const int recs = 1 + static_cast<int>(rng() % 40);
        for (int r = 0; r < recs; ++r) {
            const std::string key = (rng() % 3 == 0) ? std::string() : randomBytes(rng, rng() % 300);
            const std::string val = randomBytes(rng, sizes[rng() % std::size(sizes)] / (iter % 5 == 0 ? 1 : 8));
            const int64_t ts = base + static_cast<int64_t>(rng() % 2'000'000'000LL) - 1'000'000'000LL;
            fb.append(key, val, ts);
            ref.append(key, val, ts);
        }
        ASSERT_EQ(fb.recordCount(), ref.recordCount());
        ASSERT_EQ(fb.bodyBytes(), ref.bodyBytes());
        const int64_t pid = static_cast<int64_t>(rng());
        const auto    ep  = static_cast<int16_t>(rng());
        const auto    seq = static_cast<int32_t>(rng());
        fb.setProducerId(pid);   ref.setProducerId(pid);
        fb.setProducerEpoch(ep); ref.setProducerEpoch(ep);
        fb.setBaseSequence(seq); ref.setBaseSequence(seq);

        const rpc::Produce hdr = header(rng);
        const std::size_t len = fb.seal(hdr);
        ASSERT_EQ(len, fb.size());

        // Unsealed: the CRC field is 0 (the transport computes it on accept).
        const auto crc = ProduceFrameBuilder::frameCrc();
        ASSERT_EQ(rune::protocol::detail::getLE32(fb.data() + crc.crcAt), 0u);
        rune::client::transport::sealFrame(fb.data(), len, crc);

        const std::vector<uint8_t> batch = ref.build();
        rpc::Produce whole = hdr;
        whole.recordBatch = batch;
        std::vector<uint8_t> expect(whole.encodedSize());
        ASSERT_EQ(whole.encode(expect.data()), expect.size());
        ASSERT_EQ(std::vector<uint8_t>(fb.data(), fb.data() + len), expect) << "iter=" << iter;
    }
}

TEST(ProduceFrameBuilder, ResealAfterMoreRecordsTracksTheNewBatch) {
    // A back-pressured batch keeps accepting records; the next seal() must
    // describe the grown batch (count, length, maxTimestamp) exactly.
    ProduceFrameBuilder fb(0, 100);
    rune::protocol::RecordBatchBuilder ref(0, 100);
    rpc::Produce hdr;
    hdr.partitionKnown = 1;
    fb.append("a", "one", 150);   ref.append("a", "one", 150);
    (void)fb.seal(hdr);
    fb.append("b", "two", 300);   ref.append("b", "two", 300);
    const std::size_t len = fb.seal(hdr);
    rune::client::transport::sealFrame(fb.data(), len, ProduceFrameBuilder::frameCrc());
    const std::vector<uint8_t> batch = ref.build();
    ASSERT_EQ(len - ProduceFrameBuilder::kBatchStart, batch.size());
    EXPECT_TRUE(std::equal(batch.begin(), batch.end(), fb.data() + ProduceFrameBuilder::kBatchStart));
    rune::protocol::RecordBatchReader rd(fb.data() + ProduceFrameBuilder::kBatchStart, batch.size());
    EXPECT_TRUE(rd.ensureValid());
    EXPECT_EQ(rd.recordCount(), 2);
    EXPECT_EQ(rd.maxTimestamp(), 300);
}

TEST(ProduceFrameBuilder, SwapHandsTheFrameOverWithoutCopyingAndStartsEmpty) {
    ProduceFrameBuilder fb(0, 1);
    for (int i = 0; i < 100; ++i) fb.append("k", std::string(500, 'v'), 2);
    const std::size_t len = fb.seal(rpc::Produce{});
    const uint8_t* storage = fb.data();
    std::vector<uint8_t> retained;
    fb.swapFrameInto(retained);
    EXPECT_EQ(retained.data(), storage);  // the encoded frame itself, not a copy
    EXPECT_EQ(retained.size(), len);
    EXPECT_EQ(fb.recordCount(), 0);
    EXPECT_EQ(fb.bodyBytes(), 0u);
    EXPECT_GE(fb.size(), ProduceFrameBuilder::kRecordsStart);
}
