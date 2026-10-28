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

#include <cstring>
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
    rune::client::producer::FrameBytes retained;  // the in-flight slot buffer type
    fb.swapFrameInto(retained);
    EXPECT_EQ(retained.data(), storage);  // the encoded frame itself, not a copy
    EXPECT_EQ(retained.size(), len);
    EXPECT_EQ(fb.recordCount(), 0);
    EXPECT_EQ(fb.bodyBytes(), 0u);
    EXPECT_GE(fb.size(), ProduceFrameBuilder::kRecordsStart);
}

// Phase 2b Part B: an Invoker claim reserves a record in the frame and the caller
// writes the value in place — the sealed frame must equal append()'s exactly.
TEST(ProduceFrameBuilder, ReservedRecordFilledInPlaceIsByteIdenticalToAppend) {
    std::mt19937_64 rng(0x2B0B);
    for (int iter = 0; iter < 200; ++iter) {
        const int64_t base = 1'700'000'000'000;
        ProduceFrameBuilder a(0, base);
        ProduceFrameBuilder b(0, base);
        const int recs = 1 + static_cast<int>(rng() % 20);
        for (int r = 0; r < recs; ++r) {
            const std::string key = randomBytes(rng, rng() % 17);
            const std::string val = randomBytes(rng, rng() % 3000);
            const int64_t ts = base + static_cast<int64_t>(rng() % 100'000);
            a.append(key, val, ts);
            const std::size_t at = b.reserveRecord(key, val.size(), ts);
            std::memcpy(b.data() + at, val.data(), val.size());
        }
        ASSERT_EQ(a.recordCount(), b.recordCount());
        ASSERT_EQ(a.bodyBytes(), b.bodyBytes());
        const rpc::Produce m = header(rng);
        const std::size_t la = a.seal(m);
        const std::size_t lb = b.seal(m);
        ASSERT_EQ(std::vector<uint8_t>(a.data(), a.data() + la), std::vector<uint8_t>(b.data(), b.data() + lb))
            << "iter " << iter;
    }
}

TEST(ProduceFrameBuilder, RollbackRestoresTheExactFrameAndMaxTimestamp) {
    ProduceFrameBuilder fb(0, 1000);
    fb.append("k", "first", 1005);
    const ProduceFrameBuilder::Mark m = fb.mark();
    const std::vector<uint8_t> before(fb.data(), fb.data() + fb.size());
    (void)fb.reserveRecord("k", 40, 9000);  // a later max timestamp
    (void)fb.reserveRecord("k", 2, 9001);
    EXPECT_EQ(fb.recordCount(), 3);
    fb.rollback(m);
    EXPECT_EQ(fb.recordCount(), 1);
    EXPECT_EQ(std::vector<uint8_t>(fb.data(), fb.data() + fb.size()), before);
    ProduceFrameBuilder ref(0, 1000);
    ref.append("k", "first", 1005);
    rpc::Produce msg{};
    const std::size_t lf = fb.seal(msg);
    const std::size_t lr = ref.seal(msg);
    EXPECT_EQ(std::vector<uint8_t>(fb.data(), fb.data() + lf), std::vector<uint8_t>(ref.data(), ref.data() + lr))
        << "maxTimestamp must roll back too";
}

TEST(ProduceFrameBuilder, ReserveExtraKeepsDataStableAcrossReservations) {
    ProduceFrameBuilder fb(0, 0);
    fb.reserveExtra(10 * (100 + ProduceFrameBuilder::kRecordOverheadMax));
    const uint8_t* d = fb.data();
    for (int i = 0; i < 10; ++i) (void)fb.reserveRecord("", 100, 0);
    EXPECT_EQ(fb.data(), d) << "reservations inside reserveExtra() never reallocate";
    EXPECT_EQ(fb.recordCount(), 10);
}

// Task 10 regression: the frame buffer (FrameBytes) no longer zeroes growth, so
// every byte of a sent frame must be WRITTEN — header, record prefixes, values,
// headerCount.  Fill recycled storage with garbage (the in-flight slot buffer a
// swap hands back, and the tail a rollback leaves behind), then build through
// append + reserveRecord; the sealed frame must equal the protocol encoders'.
TEST(ProduceFrameBuilderRegression, GarbageInRecycledStorageNeverReachesTheSealedFrameRegression) {
    std::mt19937_64 rng(0x7A10);
    ProduceFrameBuilder fb(0, 1'700'000'000'000);
    for (int iter = 0; iter < 100; ++iter) {
        const int64_t base = 1'700'000'000'000 + iter;
        // Recycle: hand the frame out and take back a garbage-filled buffer
        // whose capacity covers the next batch (no reallocation hides it).
        rune::client::producer::FrameBytes slot;
        slot.resize(64 * 1024);
        for (auto& c : slot) c = static_cast<uint8_t>(rng() | 1u);
        fb.swapFrameInto(slot);
        fb.reset(0, base);
        {   // A rolled-back claim leaves garbage past the mark too.
            const ProduceFrameBuilder::Mark m = fb.mark();
            const std::size_t at = fb.reserveRecord("x", 900, base);
            std::memset(fb.data() + at, 0xEE, 900);
            fb.rollback(m);
        }
        rune::protocol::RecordBatchBuilder ref(0, base);
        const int recs = 1 + static_cast<int>(rng() % 20);
        for (int r = 0; r < recs; ++r) {
            const std::string key = (rng() % 3 == 0) ? std::string() : randomBytes(rng, rng() % 17);
            const std::string val = randomBytes(rng, rng() % 2000);
            const int64_t ts = base + static_cast<int64_t>(rng() % 100'000);
            if (r % 2 == 0) {
                fb.append(key, val, ts);
            } else {
                const std::size_t at = fb.reserveRecord(key, val.size(), ts);
                std::memcpy(fb.data() + at, val.data(), val.size());
            }
            ref.append(key, val, ts);
        }
        const rpc::Produce hdr = header(rng);
        fb.setProducerId(hdr.producerId);       ref.setProducerId(hdr.producerId);
        fb.setProducerEpoch(hdr.producerEpoch); ref.setProducerEpoch(hdr.producerEpoch);
        fb.setBaseSequence(hdr.baseSequence);   ref.setBaseSequence(hdr.baseSequence);
        const std::size_t len = fb.seal(hdr);
        rune::client::transport::sealFrame(fb.data(), len, ProduceFrameBuilder::frameCrc());
        const std::vector<uint8_t> batch = ref.build();
        rpc::Produce whole = hdr;
        whole.recordBatch = batch;
        std::vector<uint8_t> expect(whole.encodedSize());
        ASSERT_EQ(whole.encode(expect.data()), expect.size());
        ASSERT_EQ(std::vector<uint8_t>(fb.data(), fb.data() + len), expect) << "iter=" << iter;
    }
}

// #333: a rolled-back reservation leaves NO trace — the frame sealed after
// append(r1), mark, reserve(r2), rollback, append(r3) equals the frame of a builder that never saw r2
// (record count, offsetDelta of r3, maxTimestamp, bytes).
TEST(ProduceFrameBuilderRegression, AppendAfterRollbackMatchesANeverReservedBuilderRegression) {
    const int64_t base = 5000;
    rpc::Produce hdr{};
    hdr.partitionKnown = 1;
    {
        ProduceFrameBuilder x(0, base);
        x.append("k1", "value-one", base + 1);
        const ProduceFrameBuilder::Mark m = x.mark();
        const std::size_t at = x.reserveRecord("k2", 64, base + 9'000);  // later max timestamp
        std::memset(x.data() + at, 0xAB, 64);
        x.rollback(m);
        x.append("k3", "value-three", base + 3);
        ProduceFrameBuilder y(0, base);
        y.append("k1", "value-one", base + 1);
        y.append("k3", "value-three", base + 3);
        ASSERT_EQ(x.recordCount(), 2);
        ASSERT_EQ(x.recordCount(), y.recordCount());
        const std::size_t lx = x.seal(hdr);
        const std::size_t ly = y.seal(hdr);
        ASSERT_EQ(lx, ly);
        EXPECT_EQ(0, std::memcmp(x.data(), y.data(), lx));
        // r3's offsetDelta really is 1 (not 2): decode it back.
        rune::protocol::RecordBatchReader rd(x.data() + ProduceFrameBuilder::kBatchStart,
                                             lx - ProduceFrameBuilder::kBatchStart);
        EXPECT_EQ(rd.maxTimestamp(), base + 3);
        // #675: the callback's offset is baseOffset + offsetDelta; the rolled-back r2 left no gap.
        std::vector<int64_t> offsets;
        rd.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) {
            offsets.push_back(off - rd.baseOffset());
        });
        EXPECT_EQ(offsets, (std::vector<int64_t>{0, 1}));
    }
    {   // Mark taken at the empty batch.
        ProduceFrameBuilder x(0, base);
        const ProduceFrameBuilder::Mark m = x.mark();
        (void)x.reserveRecord("zz", 10, base + 77);
        x.rollback(m);
        x.append("k1", "value-one", base + 1);
        ProduceFrameBuilder y(0, base);
        y.append("k1", "value-one", base + 1);
        const std::size_t lx = x.seal(hdr);
        const std::size_t ly = y.seal(hdr);
        ASSERT_EQ(lx, ly);
        EXPECT_EQ(0, std::memcmp(x.data(), y.data(), lx));
    }
}

// #333: reserveExtra(kRecordOverheadMax + key + value) must cover the WORST record framing.  The worst a record
// can cost: a 2^27-byte value (5-byte length + 5-byte valueLen varints), a 1 MiB key (3-byte keyLen) and a
// timestamp delta of 2^62 (10-byte varlong) = 26 of the 32 bytes.  (A 5-byte varint needs a value >= 2^27; the
// frame is only resized, never touched, so the 128 MiB stays virtual.)
TEST(ProduceFrameBuilderRegression, ReserveExtraBoundHoldsAtMaximumRecordOverheadRegression) {
    struct Case { std::size_t keyLen, valueLen; int64_t tsDelta; };
    const Case cases[] = {
        {0, 0, 0},
        {1, 63, 1},
        {200, 100'000, int64_t{1} << 40},
        {1u << 20, 1u << 27, int64_t{1} << 62},
    };
    for (const Case& c : cases) {
        ProduceFrameBuilder fb(0, 0);
        const std::string key(c.keyLen, 'k');
        const std::size_t bound = ProduceFrameBuilder::kRecordOverheadMax + c.keyLen + c.valueLen;
        fb.reserveExtra(bound);
        const uint8_t*    d      = fb.data();
        const std::size_t before = fb.size();
        const std::size_t at     = fb.reserveRecord(key, c.valueLen, c.tsDelta);
        EXPECT_EQ(fb.data(), d) << "reserveExtra(bound) must keep data() stable, keyLen=" << c.keyLen;
        const std::size_t written = fb.size() - before;
        EXPECT_LE(written, bound) << "record framing exceeds kRecordOverheadMax, keyLen=" << c.keyLen;
        EXPECT_EQ(at + c.valueLen + 1, fb.size());
    }
}

// #374: after preallocate(N) nothing within N bytes moves the frame - not appends, not claim-style reservations,
// not reserveExtra() - which is what lets a producer's partition builder never allocate on the hot path.
TEST(ProduceFrameBuilderRegression, PreallocatedBuilderNeverMovesWithinItsCapacityRegression) {
    constexpr std::size_t kBytes = 32 * 1024;
    ProduceFrameBuilder fb(0, 0);
    fb.preallocate(kBytes);
    const uint8_t* const at = fb.data();
    ASSERT_NE(at, nullptr);
    std::size_t i = 0;
    while (fb.size() + 64 + ProduceFrameBuilder::kRecordOverheadMax < kBytes) {
        if (i++ % 2 == 0) {
            fb.append("key", std::string(60, 'v'), 1);
        } else {
            fb.reserveExtra(3 + 60 + ProduceFrameBuilder::kRecordOverheadMax);
            (void)fb.reserveRecord("key", 60, 1);
        }
        ASSERT_EQ(fb.data(), at) << "record " << i << ": the frame moved inside its preallocated capacity";
    }
    EXPECT_GT(fb.recordCount(), 100);
    fb.reset(0, 0);
    EXPECT_EQ(fb.data(), at) << "reset() keeps the storage";
}

// #333/#675: a Mark taken AFTER the current end (stale / larger) must be refused, not silently grow the frame over
// unwritten bytes.  Every build refuses it with a branch (no change); debug builds also assert, so the debug leg is a
// death test and the release leg checks that size and recordCount are untouched.
TEST(ProduceFrameBuilderRegression, RollbackToAStaleLargerMarkIsRefusedRegression) {
    ProduceFrameBuilder fb(0, 0);
    fb.append("k", "v", 1);
    const ProduceFrameBuilder::Mark small = fb.mark();
    (void)fb.reserveRecord("k", 500, 2);
    const ProduceFrameBuilder::Mark stale = fb.mark();
    fb.rollback(small);
    fb.reserveExtra(4096);  // the stale grow would stay inside capacity (no throw from noexcept)
    const std::size_t sizeBefore = fb.size();
    const int32_t countBefore = fb.recordCount();
#ifdef NDEBUG
    fb.rollback(stale);
    EXPECT_EQ(fb.size(), sizeBefore) << "a stale larger mark must not grow the frame";
    EXPECT_EQ(fb.recordCount(), countBefore);
#elif GTEST_HAS_DEATH_TEST
    EXPECT_DEBUG_DEATH(fb.rollback(stale), "stale Mark");
#endif
    (void)sizeBefore;
    (void)countBefore;
}
