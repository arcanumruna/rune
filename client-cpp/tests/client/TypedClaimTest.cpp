// tests/client/TypedClaimTest.cpp — Phase 2b Part B: TypedTopic::claim(key) — the
// generated PositionEncoder over a claimed record, routed exactly like
// TypedTopic::send(rec) (Part C), the bound schema version stamped, refusals
// harmless, zero allocation.  Bare ProducerImpl (Ring ingress), no Aeron.
#include "../support/AllocCounter.h"
#include "../support/FrameCapture.h"
#include "gen/position.gen.h"
#include "gen/tick_sample.gen.h"
#include "gen/long_key.gen.h"
#include "gen/short_key.gen.h"

#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace rc = rune::client;
using rc::Key;
using rc::Producer;
using rc::ProducerConfig;
using rc::SendResult;
using rc::TopicHandle;
using rc::TypedTopic;
using rc::producer::ProducerImpl;
using rune::gen::Position;
using rune::gen::PositionDecoder;
using rune::gen::PositionEncoder;
using rune::gen::LongKey;
using rune::gen::ShortKey;
using rune::gen::TickSample;
using test_support::FrameCapture;

static_assert(std::is_same_v<Position::Encoder, PositionEncoder>);
static_assert(PositionEncoder::kEncodedLength == 67u);
static_assert(!rune::gen::TickSampleEncoder::kHasKey);

namespace {
constexpr int64_t kNow = 1'000'000'000;

ProducerConfig voidCfg() {
    ProducerConfig c;
    c.acks        = rc::Acks::None;
    c.lingerUs    = 0;
    c.batchBytes  = 16 * 1024;
    c.maxInFlight = 8;
    return c;
}

void pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    for (int i = 0; i < 16; ++i) {
        impl.drainRing(kNow, 256);
        impl.flushBatches(kNow, 42, sink);
    }
}

std::span<const uint8_t> span(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}
}  // namespace

// Review Focus 1.
TEST(TypedClaim, RoutesLikeTypedSendForEveryKey) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<Position> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    for (int64_t k = 0; k < 64; ++k) {
        Position rec;
        rec.userId = k;
        rec.zone   = 1;
        topic.send(rec);
        auto w = topic.claim(k);
        ASSERT_TRUE(w) << static_cast<int>(w.result());
        w.zone(2);
        ASSERT_TRUE(w.commit());
    }
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 128u);
    std::map<int64_t, std::vector<test_support::CapturedRecord>> byUser;
    for (const auto& r : cap.recs) byUser[PositionDecoder(span(r.value)).userId()].push_back(r);
    ASSERT_EQ(byUser.size(), 64u);
    for (const auto& [k, rs] : byUser) {
        ASSERT_EQ(rs.size(), 2u) << k;
        EXPECT_EQ(rs[0].partition, rs[1].partition) << "claim(" << k << ") must route like send";
        EXPECT_EQ(rs[0].key, rs[1].key) << "same record key for userId " << k;
        EXPECT_EQ(rs[0].partition, Key(k).partition(8)) << k;
        EXPECT_EQ(PositionDecoder(span(rs[0].value)).zone(), 1) << "per-key order: send first";
        EXPECT_EQ(PositionDecoder(span(rs[1].value)).zone(), 2);
    }
}

TEST(TypedClaim, StampsTheBoundVersionAndTheKeyFieldAndRoundTripsEveryField) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<Position> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{7}};
    {
        auto w = topic.claim(int64_t{42});
        ASSERT_TRUE(w);
        w.zone(-3).x(1.5).y(-2.25).z(1e9).flags(0xBEEF).seq(4'000'000'000u).tick(0x0102030405060708ull)
            .alive(true).kind(2).tag("north");
        ASSERT_TRUE(w.commit());
    }
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 1u);
    const std::string& v = cap.recs[0].value;
    ASSERT_EQ(v.size(), PositionEncoder::kEncodedLength);
    EXPECT_EQ(static_cast<uint8_t>(v[2]), 7);
    EXPECT_EQ(static_cast<uint8_t>(v[3]), 0) << "the bound version is stamped at bytes [2..3]";
    Position back;
    back.decode(span(v));
    EXPECT_EQ(back.userId, 42) << "claim(key) writes the key field";
    EXPECT_EQ(back.zone, -3);
    EXPECT_EQ(back.x, 1.5);
    EXPECT_EQ(back.y, -2.25);
    EXPECT_EQ(back.z, 1e9);
    EXPECT_EQ(back.flags, 0xBEEF);
    EXPECT_EQ(back.seq, 4'000'000'000u);
    EXPECT_EQ(back.tick, 0x0102030405060708ull);
    EXPECT_TRUE(back.alive);
    EXPECT_EQ(back.kind, 2);
    EXPECT_EQ(std::string_view(back.tag.data(), 8), std::string_view("north\0\0\0", 8));
    const Key k42(int64_t{42});
    EXPECT_EQ(cap.recs[0].key, std::string(reinterpret_cast<const char*>(k42.bytes().data()), k42.size()));
}

TEST(TypedClaim, ARefusedClaimWritesOnlyItsScratchAndCommitsNothing) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);  // nobody drains: it fills
    TypedTopic<Position> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    int accepted = 0;
    bool refused = false;
    for (int64_t i = 0; i < 1000 && !refused; ++i) {
        auto w = topic.claim(i);
        if (!w) {
            refused = true;
            EXPECT_EQ(w.result(), SendResult::BackPressured);
            w.zone(9).x(1.0).tag("scratch");  // safe: the encoder writes its private scratch block
            EXPECT_FALSE(w.commit());
            break;
        }
        w.zone(1);
        ASSERT_TRUE(w.commit());
        ++accepted;
    }
    ASSERT_TRUE(refused) << "a 4 KiB ring must fill";
    ASSERT_GT(accepted, 0);
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.recs.size(), static_cast<std::size_t>(accepted));
    for (const auto& r : cap.recs) EXPECT_EQ(PositionDecoder(span(r.value)).zone(), 1);
}

TEST(TypedClaim, AKeylessSchemaClaimsPartitionZeroWithAnEmptyKey) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<TickSample> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    for (uint64_t i = 0; i < 4; ++i) {
        auto w = topic.claim();
        ASSERT_TRUE(w);
        w.seq(i).value(0.5);
        ASSERT_TRUE(w.commit());
    }
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 4u);
    for (const auto& r : cap.recs) {
        EXPECT_EQ(r.partition, 0) << "keyless typed topics keep partition 0 (spec C)";
        EXPECT_TRUE(r.key.empty());
    }
}

TEST(TypedClaimAllocRegression, TypedClaimSteadyStateAllocatesNothingRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<Position> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](int64_t k) {
        {
            auto w = topic.claim(k);
            if (w) {
                w.zone(1).x(2.0);
                (void)w.commit();
            }
        }
        pump(*impl, sink);
    };
    for (int64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (int64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "typed claim + commit + drain/flush must not allocate";
}

TEST(TypedClaimRegression, OverlongStringKeyThrowsAndClaimsNothingRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<LongKey> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    EXPECT_THROW((void)topic.claim(std::string(17, 'x')), std::invalid_argument);
    {
        FrameCapture cap;
        pump(*impl, cap.fn());
        EXPECT_TRUE(cap.recs.empty()) << "a throwing claim must leave no record";
    }
    // Nothing was claimed: a 16-byte key still routes, with the exact bytes as the record key.
    const std::string k16(16, 'x');
    {
        auto w = topic.claim(k16);
        ASSERT_TRUE(w);
        w.v(5);
        ASSERT_TRUE(w.commit());
    }
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 1u) << "exactly the 16-byte claim, no ghost slot from the refused one";
    EXPECT_EQ(cap.recs[0].key, k16);
    EXPECT_EQ(cap.recs[0].partition,
              Key(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k16.data()), k16.size())).partition(8));
    LongKey back;
    back.decode(span(cap.recs[0].value));
    EXPECT_EQ(std::string_view(back.code.data(), 16), k16);
    EXPECT_EQ(back.v, 5);
}

// string(N <= 16) key: claim(std::string_view) must equal send(rec) byte for byte.
TEST(TypedClaim, StringKeyClaimMatchesSendForShortKeys) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<ShortKey> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    const std::string_view keys[] = {"",       "a",        "abcdefgh", "abc",
                                     "ab",     std::string_view("ab\0\0", 4), "zzzzzzz", "k1"};
    for (const auto k : keys) {
        ShortKey rec;
        rec.code.fill('\0');
        std::memcpy(rec.code.data(), k.data(), k.size());
        rec.v = 77;
        topic.send(rec);
        auto w = topic.claim(k);
        ASSERT_TRUE(w);
        w.v(77);
        ASSERT_TRUE(w.commit());
    }
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 16u);
    // Per partition, send/claim pairs stay in order, but partitions interleave: match by key.
    std::map<std::string, std::vector<test_support::CapturedRecord>> byKey;
    for (const auto& r : cap.recs) byKey[r.value.substr(4, 8)].push_back(r);
    EXPECT_EQ(byKey.size(), 7u) << "\"ab\" and \"ab\\0\\0\" are the same padded key";
    for (const auto& [field, rs] : byKey) {
        ASSERT_EQ(rs.size() % 2, 0u);
        for (std::size_t i = 0; i + 1 < rs.size(); i += 2) {
            EXPECT_EQ(rs[i].partition, rs[i + 1].partition) << field;
            EXPECT_EQ(rs[i].key, rs[i + 1].key) << field;
            EXPECT_EQ(rs[i].key.size(), 8u) << "record key = the 8 zero-padded field bytes";
            EXPECT_EQ(rs[i].value, rs[i + 1].value) << "full value bytes";
        }
    }
}

TEST(TypedClaimRegression, StringKeyLongerThanFieldThrowsAndClaimsNothingRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<ShortKey> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    EXPECT_THROW((void)topic.claim(std::string(9, 'x')), std::invalid_argument);
    EXPECT_THROW((void)topic.claim(std::string(17, 'x')), std::invalid_argument);
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_TRUE(cap.recs.empty());
}

// The routed key cannot be rewritten through a claim (silent-misroute class).
template <class W>
concept CanSetKey = requires(W& w) { w.key(int64_t{1}); } || requires(W& w) { w.userId(int64_t{1}); } ||
                    requires(const W& w) { w.key(); };
using PositionClaim = decltype(std::declval<TypedTopic<Position>&>().claim(int64_t{}));
static_assert(!CanSetKey<PositionClaim>, "the key field must not be reachable through a TypedClaim");
static_assert(requires(PositionClaim& w) { w.zone(1); }, "non-key setters stay available");
