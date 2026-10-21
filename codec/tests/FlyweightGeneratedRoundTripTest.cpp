// tests/FlyweightGeneratedRoundTripTest.cpp — Phase 2b Part B: the generated
// AllfixedSampleEncoder / AllfixedSampleDecoder (build-time generated from
// allfixed_sample.blob) against the generated struct's encode() / decode():
// byte-identical output, field-identical reads, every fixed type, odd offsets.
#include <gtest/gtest.h>
#include "generated/allfixed_sample.gen.h"   // generated at build time
#include <rune/codec/Flyweight.h>
#include <rune/codec/Validator.h>

#include <cstring>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <vector>

using rune::gen::AllfixedSample;
using Enc = rune::gen::AllfixedSampleEncoder;
using Dec = rune::gen::AllfixedSampleDecoder;

static_assert(std::is_same_v<AllfixedSample::Encoder, Enc>);
static_assert(std::is_same_v<AllfixedSample::Decoder, Dec>);
static_assert(Enc::kBlockLength == 45u);
static_assert(Enc::kEncodedLength == 49u);
static_assert(Enc::kHasKey && Dec::kHasKey);
static_assert(std::is_same_v<Enc::KeyType, int64_t>);

namespace {
AllfixedSample sample() {
    AllfixedSample s;
    s.id = -1'234'567'890'123LL;
    s.a  = -42;
    s.b  = 65535;
    s.c  = 4'000'000'000u;
    s.d  = 0x0123456789ABCDEFull;
    s.e  = -3.25;
    s.f  = true;
    s.g  = 2;
    std::memcpy(s.code.data(), "ABC", 3);  // the rest stays zero
    return s;
}
std::vector<uint8_t> encoded(const AllfixedSample& s) {
    std::vector<uint8_t> v(s.encodedSize());
    v.resize(s.encode(v.data()));
    return v;
}
}  // namespace

TEST(FlyweightGen, EncoderBytesEqualStructEncode) {
    const AllfixedSample s = sample();
    const std::vector<uint8_t> want = encoded(s);
    std::vector<uint8_t> got(Enc::kEncodedLength, 0xEE);  // dirty memory, like a reused ring slot
    Enc(got).id(s.id).a(s.a).b(s.b).c(s.c).d(s.d).e(s.e).f(s.f).g(s.g).code("ABC");
    EXPECT_EQ(got, want);
    EXPECT_EQ(want.size(), static_cast<std::size_t>(Enc::kEncodedLength));
    EXPECT_EQ(Enc::kBlockLength, static_cast<uint32_t>(AllfixedSample::kBlockLength));
    EXPECT_EQ(Enc::kVersion, AllfixedSample::kVersion);
}

TEST(FlyweightGen, DecoderReadsWhatDecodeReads) {
    const std::vector<uint8_t> rec = encoded(sample());
    AllfixedSample back;
    ASSERT_TRUE(back.decode(rec));
    const Dec dec(rec);
    EXPECT_EQ(dec.id(), back.id);
    EXPECT_EQ(dec.a(), back.a);
    EXPECT_EQ(dec.b(), back.b);
    EXPECT_EQ(dec.c(), back.c);
    EXPECT_EQ(dec.d(), back.d);
    EXPECT_EQ(dec.e(), back.e);
    EXPECT_EQ(dec.f(), back.f);
    EXPECT_EQ(dec.g(), back.g);
    EXPECT_EQ(dec.code(), std::string_view(back.code.data(), 6));
    EXPECT_EQ(dec.key(), back.id);
}

TEST(FlyweightGen, EncoderZeroesTheBlockOverDirtyMemory) {
    std::vector<uint8_t> buf(Enc::kEncodedLength, 0xEE);
    Enc(buf).id(7);
    const Dec dec(buf);
    EXPECT_EQ(dec.id(), 7);
    EXPECT_EQ(dec.a(), 0);
    EXPECT_EQ(dec.d(), 0u);
    EXPECT_EQ(dec.e(), 0.0);
    EXPECT_FALSE(dec.f());
    EXPECT_EQ(dec.code(), std::string_view("\0\0\0\0\0\0", 6));
}

// Review Focus 4: the published docs claim PositionEncoder::kBlockLength bytes;
// the encoder needs kEncodedLength (4 header bytes more) and must refuse loudly
// instead of writing past the claim.
TEST(FlyweightGen, EncoderRejectsABufferShorterThanKEncodedLength) {
    std::vector<uint8_t> buf(Enc::kBlockLength, 0xEE);
    EXPECT_THROW(Enc{std::span<uint8_t>(buf)}, std::length_error);
    for (const uint8_t b : buf) EXPECT_EQ(b, 0xEE) << "nothing written";
}

TEST(FlyweightGen, FixedStrSetterPadsAndTruncates) {
    std::vector<uint8_t> buf(Enc::kEncodedLength);
    Enc(buf).code("TOO-LONG-VALUE");
    EXPECT_EQ(Dec(buf).code(), std::string_view("TOO-LO", 6));
    Enc(buf).code("Z");
    EXPECT_EQ(Dec(buf).code(), std::string_view("Z\0\0\0\0\0", 6));
}

TEST(FlyweightGen, DecoderToleratesNewerLongerAndOlderShorterBlocks) {
    const AllfixedSample s = sample();
    const std::vector<uint8_t> rec = encoded(s);
    // A newer version: its block is 8 bytes longer (a field appended at the end).
    std::vector<uint8_t> newer = rec;
    newer.insert(newer.end(), 8, uint8_t{0x55});
    rune::codec::fly::store<uint16_t>(newer.data(), static_cast<uint16_t>(Enc::kBlockLength + 8));
    const Dec dn(newer);
    EXPECT_EQ(dn.id(), s.id);
    EXPECT_EQ(dn.g(), s.g);
    EXPECT_EQ(dn.code(), std::string_view(s.code.data(), 6));
    // An older version whose block held only `id` (8 bytes).
    std::vector<uint8_t> older(4 + 8);
    rune::codec::fly::store<uint16_t>(older.data(), uint16_t{8});
    rune::codec::fly::store<uint16_t>(older.data() + 2, uint16_t{1});
    rune::codec::fly::store<int64_t>(older.data() + 4, s.id);
    const Dec dold(older);
    EXPECT_EQ(dold.id(), s.id);
    EXPECT_EQ(dold.a(), 0);
    EXPECT_FALSE(dold.f());
    EXPECT_TRUE(dold.code().empty());
    // Truncated input never reads out of bounds.
    const Dec dt(std::span<const uint8_t>(rec.data(), 10));
    EXPECT_EQ(dt.id(), 0) << "id @0..8 needs bytes 4..12 of a 10-byte input";
    EXPECT_EQ(dt.a(), 0);
}

TEST(FlyweightGen, EncoderOutputPassesTheValidator) {
    const AllfixedSample s = sample();
    std::vector<uint8_t> got(Enc::kEncodedLength);
    Enc(got).id(s.id).a(s.a).b(s.b).c(s.c).d(s.d).e(s.e).f(s.f).g(s.g).code("ABC");
    EXPECT_TRUE(rune::codec::Validator(AllfixedSample::descriptor()).validate(got).ok);
}

TEST(FlyweightGen, KeyHelpersAreTheKeyField) {
    std::vector<uint8_t> buf(Enc::kEncodedLength);
    Enc e(buf);
    e.key(99);
    EXPECT_EQ(e.key(), 99);
    EXPECT_EQ(Dec(buf).id(), 99);
    EXPECT_EQ(Dec(buf).key(), 99);
}
