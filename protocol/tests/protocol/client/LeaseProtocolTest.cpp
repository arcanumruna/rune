// LeaseProtocolTest.cpp — lease topics Task 1: op bits, predicates, LeaseSpec, codes, templates 96-101, the
// ProduceResponse / DefineTopic / TopicSchemaResponse lease trailers, and the pinned numbers.
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/client/CryptoEnvelope.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/LeaseMessages.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace rp = rune::protocol;
namespace rc = rune::protocol::client;
namespace rr = rune::protocol::record;

namespace {

using Bytes = std::vector<uint8_t>;

Bytes keyOf(std::size_t n, uint8_t seed = 1) {
    Bytes k(n);
    for (std::size_t i = 0; i < n; ++i) k[i] = static_cast<uint8_t>(seed + i * 7);
    return k;
}

std::span<const uint8_t> sp(const Bytes& b) { return {b.data(), b.size()}; }

template <class T>
Bytes encodeVec(const T& m) {
    Bytes b(m.encodedSize());
    EXPECT_EQ(m.encode(b.data()), b.size());
    return b;
}

std::string hexOf(const uint8_t* p, std::size_t n) {
    static const char* d = "0123456789abcdef";
    std::string s;
    for (std::size_t i = 0; i < n; ++i) { s += d[p[i] >> 4]; s += d[p[i] & 15]; }
    return s;
}

// A keep-alive frame with the given keys (version = index).
Bytes keepAliveFrame(const std::vector<Bytes>& keys) {
    std::vector<rc::LeaseKeepAlive::Entry> es;
    for (std::size_t i = 0; i < keys.size(); ++i) es.push_back({sp(keys[i]), static_cast<int64_t>(i) + 1});
    rc::LeaseKeepAlive m;
    m.sessionId = 11; m.correlationId = 22; m.topicId = 5; m.partition = 3;
    m.entries = std::span<const rc::LeaseKeepAlive::Entry>(es.data(), es.size());
    return encodeVec(m);
}

} // namespace

// ---------------------------------------------------------------------------
// record attributes
// ---------------------------------------------------------------------------
TEST(LeaseProtocolRegression, OpBitsRoundTripRegression) {
    for (const auto op : {rr::LeaseOp::Put, rr::LeaseOp::Revoke, rr::LeaseOp::Expire, rr::LeaseOp::Checkpoint}) {
        for (int a = 0; a < 256; ++a) {
            const auto attrs = static_cast<uint8_t>(a);
            const uint8_t w = rr::withLeaseOp(attrs, op);
            EXPECT_EQ(rr::leaseOpOf(w), op) << a;
            EXPECT_EQ(w & ~0x0C, attrs & ~0x0C) << a;
        }
    }
}

TEST(LeaseProtocolRegression, PredicatesRegression) {
    // Non-lease: only an empty value is a tombstone, attrs are ignored; never control.
    for (int a = 0; a < 256; ++a) {
        const auto attrs = static_cast<uint8_t>(a);
        EXPECT_TRUE(rr::isTombstone(attrs, 0, {false})) << a;
        EXPECT_FALSE(rr::isTombstone(attrs, 1, {false})) << a;
        EXPECT_FALSE(rr::isTombstone(attrs, 36, {false})) << a;
        EXPECT_FALSE(rr::isLeaseControl(attrs, {false})) << a;
    }
    using Op = rr::LeaseOp;
    struct Row { Op op; std::size_t len; bool tomb; bool control; };
    const Row rows[] = {
        {Op::Revoke, 36, true, false},     {Op::Expire, 36, true, false},   {Op::Put, 36, false, false},
        {Op::Put, 0, false, false},        {Op::Checkpoint, 36, false, true}, {Op::Checkpoint, 0, false, true},
        {Op::Revoke, 0, true, false},      {Op::Expire, 0, true, false},
    };
    for (const Row& r : rows) {
        for (uint8_t intent = 0; intent < 4; ++intent) {
            const uint8_t attrs = rr::withLeaseOp(intent, r.op);
            EXPECT_EQ(rr::isTombstone(attrs, r.len, {true}), r.tomb) << int(r.op) << " " << r.len;
            EXPECT_EQ(rr::isLeaseControl(attrs, {true}), r.control) << int(r.op) << " " << r.len;
        }
    }
}

TEST(LeaseProtocolRegression, NonLeaseGoldenRegression) {
    std::mt19937 rng(0x1EA5E);
    for (int rec = 0; rec < 64; ++rec) {
        const std::size_t len = rng() % 65;
        for (int a = 0; a < 256; ++a)
            EXPECT_EQ(rr::isTombstone(static_cast<uint8_t>(a), len, {false}), len == 0) << rec << " " << a;
    }
}

// ---------------------------------------------------------------------------
// messages 96-101
// ---------------------------------------------------------------------------
TEST(LeaseProtocolRegression, KeepAliveRoundTripRegression) {
    for (const std::size_t count : {std::size_t{1}, std::size_t{256}}) {
        for (const std::size_t klen : {std::size_t{0}, std::size_t{17}, std::size_t{128}}) {
            std::vector<Bytes> keys;
            for (std::size_t i = 0; i < count; ++i) keys.push_back(keyOf(klen, static_cast<uint8_t>(i)));
            const Bytes frame = keepAliveFrame(keys);
            const auto d = rc::LeaseKeepAlive::decode(frame.data(), frame.size());
            ASSERT_TRUE(d.has_value()) << count << " " << klen;
            EXPECT_EQ(d->sessionId, 11);
            EXPECT_EQ(d->correlationId, 22);
            EXPECT_EQ(d->topicId, 5);
            EXPECT_EQ(d->partition, 3);
            EXPECT_EQ(d->count, count);
            std::size_t seen = 0;
            d->forEachEntry([&](uint16_t i, std::span<const uint8_t> key, int64_t version) {
                ASSERT_EQ(i, seen);
                ASSERT_EQ(key.size(), klen);
                if (klen) EXPECT_EQ(std::memcmp(key.data(), keys[i].data(), klen), 0);
                EXPECT_EQ(version, static_cast<int64_t>(i) + 1);
                // zero-copy: the key points into the frame
                EXPECT_GE(key.data(), frame.data());
                EXPECT_LE(key.data() + key.size(), frame.data() + frame.size());
                ++seen;
            });
            EXPECT_EQ(seen, count);
        }
    }
    // count 0 and 257 refused by fits() and encode()
    Bytes scratch(100000);
    rc::LeaseKeepAlive none;
    EXPECT_FALSE(none.fits());
    EXPECT_EQ(none.encode(scratch.data()), 0u);
    std::vector<rc::LeaseKeepAlive::Entry> many(257, rc::LeaseKeepAlive::Entry{{}, 1});
    rc::LeaseKeepAlive m257;
    m257.entries = std::span<const rc::LeaseKeepAlive::Entry>(many.data(), many.size());
    EXPECT_FALSE(m257.fits());
    EXPECT_EQ(m257.encode(scratch.data()), 0u);
    // a 129-byte key refused by fits()
    const Bytes big = keyOf(129);
    rc::LeaseKeepAlive::Entry e{sp(big), 1};
    rc::LeaseKeepAlive mk;
    mk.entries = std::span<const rc::LeaseKeepAlive::Entry>(&e, 1);
    EXPECT_FALSE(mk.fits());
    EXPECT_EQ(mk.encode(scratch.data()), 0u);
    // ... and by decode of a hand-built frame: header + fixed + 1 entry with keyLen 129
    Bytes hb(24 + 10 + 1 + 129 + 8, 0);
    rc::MessageHeader h; h.blockLength = 10; h.templateId = 96; h.encode(hb.data());
    hb[24 + 8] = 1;     // count = 1
    hb[34] = 129;
    EXPECT_FALSE(rc::LeaseKeepAlive::decode(hb.data(), hb.size()).has_value());
    hb[34] = 128;       // the same frame with a 128-byte key + 9 stray bytes is also refused
    EXPECT_FALSE(rc::LeaseKeepAlive::decode(hb.data(), hb.size()).has_value());
    // hand-built count 257 and count 0
    Bytes c0 = keepAliveFrame({keyOf(1)});
    c0[24 + 8] = 0; c0[24 + 9] = 0;
    EXPECT_FALSE(rc::LeaseKeepAlive::decode(c0.data(), c0.size()).has_value());
    Bytes c257 = keepAliveFrame({keyOf(1)});
    c257[24 + 8] = 0x01; c257[24 + 9] = 0x01;
    EXPECT_FALSE(rc::LeaseKeepAlive::decode(c257.data(), c257.size()).has_value());
}

TEST(LeaseProtocolRegression, KeepAliveResponseRegression) {
    for (const uint8_t fin : {uint8_t{0}, uint8_t{1}}) {
        for (const std::size_t count : {std::size_t{1}, std::size_t{256}}) {
            std::vector<rc::LeaseKeepAliveResponse::Entry> es;
            for (std::size_t i = 0; i < count; ++i)
                es.push_back({static_cast<uint16_t>(i), static_cast<uint16_t>(i % 3), 1000u + i * 0x100000001ull});
            rc::LeaseKeepAliveResponse m;
            m.sessionId = 1; m.correlationId = 2; m.final = fin;
            m.entries = std::span<const rc::LeaseKeepAliveResponse::Entry>(es.data(), es.size());
            const Bytes f = encodeVec(m);
            const auto d = rc::LeaseKeepAliveResponse::decode(f.data(), f.size());
            ASSERT_TRUE(d.has_value());
            EXPECT_EQ(d->final, fin);
            EXPECT_EQ(d->count, count);
            std::size_t seen = 0;
            d->forEachEntry([&](uint16_t idx, uint16_t code, uint64_t ttl) {
                EXPECT_EQ(idx, es[seen].index);
                EXPECT_EQ(code, es[seen].code);
                EXPECT_EQ(ttl, es[seen].ttlMs);
                ++seen;
            });
            EXPECT_EQ(seen, count);
        }
    }
    std::vector<rc::LeaseKeepAliveResponse::Entry> one(1), many(257);
    Bytes scratch(4000);
    rc::LeaseKeepAliveResponse bad;
    bad.entries = std::span<const rc::LeaseKeepAliveResponse::Entry>(one.data(), 1);
    bad.final = 2;
    EXPECT_FALSE(bad.fits());
    EXPECT_EQ(bad.encode(scratch.data()), 0u);
    bad.final = 0;
    bad.entries = std::span<const rc::LeaseKeepAliveResponse::Entry>(many.data(), 257);
    EXPECT_FALSE(bad.fits());
    EXPECT_EQ(bad.encode(scratch.data()), 0u);
    // decode: final 2, count 257, count 0 refused
    rc::LeaseKeepAliveResponse ok;
    ok.entries = std::span<const rc::LeaseKeepAliveResponse::Entry>(one.data(), 1);
    Bytes f = encodeVec(ok);
    f[24] = 2;
    EXPECT_FALSE(rc::LeaseKeepAliveResponse::decode(f.data(), f.size()).has_value());
    f[24] = 0; f[25] = 0; f[26] = 0;
    EXPECT_FALSE(rc::LeaseKeepAliveResponse::decode(f.data(), f.size()).has_value());
    Bytes f257(24 + 3 + 12 * 257, 0);
    rc::MessageHeader h; h.blockLength = 3; h.templateId = 97; h.encode(f257.data());
    f257[25] = 0x01; f257[26] = 0x01;
    EXPECT_FALSE(rc::LeaseKeepAliveResponse::decode(f257.data(), f257.size()).has_value());
}

TEST(LeaseProtocolRegression, WholeRequestIndexRegression) {
    EXPECT_EQ(rc::kLeaseWholeRequest, 0xFFFFu);
    const rc::LeaseKeepAliveResponse::Entry e{rc::kLeaseWholeRequest, static_cast<uint16_t>(rc::ErrorCode::NotLeader), 77};
    rc::LeaseKeepAliveResponse m;
    m.sessionId = 3; m.correlationId = 4; m.final = 1;
    m.entries = std::span<const rc::LeaseKeepAliveResponse::Entry>(&e, 1);
    const Bytes f = encodeVec(m);
    const auto d = rc::LeaseKeepAliveResponse::decode(f.data(), f.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->final, 1);
    ASSERT_EQ(d->count, 1);
    std::size_t seen = 0;
    d->forEachEntry([&](uint16_t idx, uint16_t code, uint64_t ttl) {
        EXPECT_EQ(idx, 0xFFFF);
        EXPECT_EQ(code, static_cast<uint16_t>(rc::ErrorCode::NotLeader));
        EXPECT_EQ(ttl, 77u);
        ++seen;
    });
    EXPECT_EQ(seen, 1u);
}

TEST(LeaseProtocolRegression, ForEachEntryOverRawBytesRegression) {
    const std::vector<Bytes> keys{keyOf(1, 3), keyOf(17, 5), keyOf(128, 9)};
    const Bytes f = keepAliveFrame(keys);
    const auto d = rc::LeaseKeepAlive::decode(f.data(), f.size());
    ASSERT_TRUE(d.has_value());
    std::vector<std::pair<Bytes, int64_t>> viaMember, viaRaw;
    d->forEachEntry([&](uint16_t i, std::span<const uint8_t> k, int64_t v) {
        EXPECT_EQ(i, viaMember.size());
        viaMember.push_back({Bytes(k.begin(), k.end()), v});
    });
    EXPECT_TRUE(rc::forEachLeaseKeepAliveEntry(d->entryBytes, d->count, [&](uint16_t i, std::span<const uint8_t> k, int64_t v) {
        EXPECT_EQ(i, viaRaw.size());
        viaRaw.push_back({Bytes(k.begin(), k.end()), v});
    }));
    ASSERT_EQ(viaRaw.size(), 3u);
    EXPECT_EQ(viaRaw, viaMember);
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(viaRaw[i].first, keys[i]);
        EXPECT_EQ(viaRaw[i].second, static_cast<int64_t>(i) + 1);
    }
}

// R3: the raw walk validates the whole span before it calls fn: a 3-entry span cut at EVERY offset, one extra trailing byte, a wrong count
// and an over-long key all return false and never call fn.
TEST(LeaseProtocolRegression, ForEachEntryRejectsTruncationRegression) {
    const std::vector<Bytes> keys{keyOf(1, 3), keyOf(17, 5), keyOf(128, 9)};
    const Bytes f = keepAliveFrame(keys);
    const auto d = rc::LeaseKeepAlive::decode(f.data(), f.size());
    ASSERT_TRUE(d.has_value());
    const Bytes whole(d->entryBytes.begin(), d->entryBytes.end());
    int calls = 0;
    auto fn = [&](uint16_t, std::span<const uint8_t>, int64_t) { ++calls; };
    EXPECT_TRUE(rc::forEachLeaseKeepAliveEntry(std::span<const uint8_t>(whole), 3, fn));
    EXPECT_EQ(calls, 3);
    calls = 0;
    for (std::size_t cut = 0; cut < whole.size(); ++cut)
        EXPECT_FALSE(rc::forEachLeaseKeepAliveEntry(std::span<const uint8_t>(whole.data(), cut), 3, fn)) << "cut at " << cut;
    Bytes extra = whole;
    extra.push_back(0);
    EXPECT_FALSE(rc::forEachLeaseKeepAliveEntry(std::span<const uint8_t>(extra), 3, fn)) << "one trailing byte";
    EXPECT_FALSE(rc::forEachLeaseKeepAliveEntry(std::span<const uint8_t>(whole), 2, fn)) << "count smaller than the entries";
    EXPECT_FALSE(rc::forEachLeaseKeepAliveEntry(std::span<const uint8_t>(whole), 4, fn)) << "count larger than the entries";
    Bytes longKey(1u + 129u + 8u, 0);
    longKey[0] = 129;
    EXPECT_FALSE(rc::forEachLeaseKeepAliveEntry(std::span<const uint8_t>(longKey), 1, fn)) << "key over kMaxLeaseKeyBytes";
    EXPECT_EQ(calls, 0) << "fn is never called for a bad span";
}

TEST(LeaseProtocolRegression, TimeToLiveRoundTripRegression) {
    for (const std::size_t klen : {std::size_t{0}, std::size_t{128}}) {
        const Bytes k = keyOf(klen);
        rc::LeaseTimeToLive m;
        m.sessionId = 3; m.correlationId = 4; m.topicId = -9; m.partition = 12; m.key = sp(k);
        const Bytes f = encodeVec(m);
        const auto d = rc::LeaseTimeToLive::decode(f.data(), f.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->topicId, -9);
        EXPECT_EQ(d->partition, 12);
        ASSERT_EQ(d->key.size(), klen);
        if (klen) EXPECT_EQ(std::memcmp(d->key.data(), k.data(), klen), 0);
    }
    const Bytes big = keyOf(129);
    rc::LeaseTimeToLive m;
    m.key = sp(big);
    Bytes scratch(400);
    EXPECT_FALSE(m.fits());
    EXPECT_EQ(m.encode(scratch.data()), 0u);
    // decode of a hand-built 129-byte key
    Bytes hb(24 + 8 + 1 + 129, 0);
    rc::MessageHeader h; h.blockLength = 8; h.templateId = 98; h.encode(hb.data());
    hb[32] = 129;
    EXPECT_FALSE(rc::LeaseTimeToLive::decode(hb.data(), hb.size()).has_value());

    rc::LeaseTimeToLiveResponse r;
    r.sessionId = 5; r.correlationId = 6; r.code = 64; r.grantedTtlMs = 30000; r.remainingMs = -1;
    r.version = 77; r.holder = 0xDEADBEEFCAFEF00Dull;
    const Bytes rf = encodeVec(r);
    const auto rd = rc::LeaseTimeToLiveResponse::decode(rf.data(), rf.size());
    ASSERT_TRUE(rd.has_value());
    EXPECT_EQ(rd->code, 64);
    EXPECT_EQ(rd->grantedTtlMs, 30000u);
    EXPECT_EQ(rd->remainingMs, -1);
    EXPECT_EQ(rd->version, 77);
    EXPECT_EQ(rd->holder, 0xDEADBEEFCAFEF00Dull);
}

TEST(LeaseProtocolRegression, ScanLeasesBoundsRegression) {
    const auto make = [](uint16_t maxRows, std::size_t prefixLen, uint8_t first, const Bytes& pfx) {
        rc::ScanLeases m;
        m.sessionId = 1; m.correlationId = 2; m.viewTopicId = 7; m.partition = 1;
        m.resumeHash = 0x0123456789ABCDEFull; m.first = first; m.maxRows = maxRows;
        m.prefix = std::span<const uint8_t>(pfx.data(), prefixLen);
        return m;
    };
    const Bytes pfx = keyOf(200);
    Bytes scratch(600);
    for (const uint16_t rows : {uint16_t{1}, uint16_t{1024}}) {
        const auto m = make(rows, 5, 1, pfx);
        ASSERT_TRUE(m.fits());
        const Bytes f = encodeVec(m);
        const auto d = rc::ScanLeases::decode(f.data(), f.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->maxRows, rows);
        EXPECT_EQ(d->resumeHash, 0x0123456789ABCDEFull);
        EXPECT_EQ(d->first, 1);
        EXPECT_EQ(d->viewTopicId, 7);
        EXPECT_EQ(d->prefix.size(), 5u);
    }
    for (const uint16_t rows : {uint16_t{0}, uint16_t{1025}}) {
        const auto m = make(rows, 0, 1, pfx);
        EXPECT_FALSE(m.fits());
        EXPECT_EQ(m.encode(scratch.data()), 0u);
        Bytes f = encodeVec(make(1, 0, 1, pfx));
        f[24 + 17] = static_cast<uint8_t>(rows); f[24 + 18] = static_cast<uint8_t>(rows >> 8);
        EXPECT_FALSE(rc::ScanLeases::decode(f.data(), f.size()).has_value()) << rows;
    }
    for (const std::size_t plen : {std::size_t{0}, std::size_t{127}}) {
        const auto m = make(10, plen, 0, pfx);
        ASSERT_TRUE(m.fits());
        const Bytes f = encodeVec(m);
        const auto d = rc::ScanLeases::decode(f.data(), f.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->prefix.size(), plen);
        EXPECT_EQ(d->first, 0);
    }
    {
        const auto m = make(10, 128, 1, pfx);
        EXPECT_FALSE(m.fits());
        EXPECT_EQ(m.encode(scratch.data()), 0u);
        Bytes f(24 + 19 + 1 + 128, 0);
        rc::MessageHeader h; h.blockLength = 19; h.templateId = 100; h.encode(f.data());
        f[24 + 16] = 1; f[24 + 17] = 10; f[24 + 19] = 128;
        EXPECT_FALSE(rc::ScanLeases::decode(f.data(), f.size()).has_value());
    }
    {
        const auto m = make(10, 0, 2, pfx);
        EXPECT_FALSE(m.fits());
        EXPECT_EQ(m.encode(scratch.data()), 0u);
        Bytes f = encodeVec(make(10, 0, 1, pfx));
        f[24 + 16] = 2;
        EXPECT_FALSE(rc::ScanLeases::decode(f.data(), f.size()).has_value());
    }
    // response round trip + bounds
    const Bytes rows = keyOf(50);
    rc::ScanLeasesResponse r;
    r.sessionId = 1; r.correlationId = 2; r.code = 0; r.asOfOffset = 99; r.lastHash = 0xFEEDull; r.done = 1;
    r.rowCount = 3; r.rows = sp(rows);
    const Bytes rf = encodeVec(r);
    const auto rd = rc::ScanLeasesResponse::decode(rf.data(), rf.size());
    ASSERT_TRUE(rd.has_value());
    EXPECT_EQ(rd->asOfOffset, 99);
    EXPECT_EQ(rd->lastHash, 0xFEEDull);
    EXPECT_EQ(rd->done, 1);
    EXPECT_EQ(rd->rowCount, 3);
    ASSERT_EQ(rd->rows.size(), rows.size());
    EXPECT_EQ(std::memcmp(rd->rows.data(), rows.data(), rows.size()), 0);
    Bytes bad = rf;
    bad[24 + 18] = 2;   // done = 2
    EXPECT_FALSE(rc::ScanLeasesResponse::decode(bad.data(), bad.size()).has_value());
}

TEST(LeaseProtocolRegression, TruncatedAtEveryOffsetRegression) {
    const Bytes k1 = keyOf(5), k2 = keyOf(128), rows = keyOf(20);
    std::vector<Bytes> frames;
    frames.push_back(keepAliveFrame({k1, k2}));
    {
        const rc::LeaseKeepAliveResponse::Entry es[2] = {{0, 0, 100}, {1, 64, 0}};
        rc::LeaseKeepAliveResponse m; m.final = 1; m.entries = std::span<const rc::LeaseKeepAliveResponse::Entry>(es, 2);
        frames.push_back(encodeVec(m));
    }
    { rc::LeaseTimeToLive m; m.key = sp(k1); frames.push_back(encodeVec(m)); }
    { rc::LeaseTimeToLiveResponse m; m.remainingMs = 5; frames.push_back(encodeVec(m)); }
    { rc::ScanLeases m; m.maxRows = 10; m.prefix = sp(k1); frames.push_back(encodeVec(m)); }
    { rc::ScanLeasesResponse m; m.rows = sp(rows); m.rowCount = 1; frames.push_back(encodeVec(m)); }
    const auto decodes = [](std::size_t which, const uint8_t* p, std::size_t n) -> bool {
        switch (which) {
            case 0: return rc::LeaseKeepAlive::decode(p, n).has_value();
            case 1: return rc::LeaseKeepAliveResponse::decode(p, n).has_value();
            case 2: return rc::LeaseTimeToLive::decode(p, n).has_value();
            case 3: return rc::LeaseTimeToLiveResponse::decode(p, n).has_value();
            case 4: return rc::ScanLeases::decode(p, n).has_value();
            default: return rc::ScanLeasesResponse::decode(p, n).has_value();
        }
    };
    for (std::size_t w = 0; w < frames.size(); ++w) {
        const Bytes& f = frames[w];
        ASSERT_TRUE(decodes(w, f.data(), f.size())) << w;
        for (std::size_t n = 0; n < f.size(); ++n) {
            // exact-size copy so ASan sees any read past `n`
            const Bytes cut(f.begin(), f.begin() + static_cast<std::ptrdiff_t>(n));
            EXPECT_FALSE(decodes(w, cut.data(), cut.size())) << "msg " << w << " cut " << n;
        }
        Bytes more = f;
        more.push_back(0);
        EXPECT_FALSE(decodes(w, more.data(), more.size())) << "msg " << w << " +1";
    }
}

// ---------------------------------------------------------------------------
// ProduceResponse lease trailer
// ---------------------------------------------------------------------------
namespace {
rc::ProduceResponse baseResponse() {
    rc::ProduceResponse r;
    r.sessionId = 7; r.correlationId = 9; r.errorCode = 0; r.partition = 4; r.baseOffset = 1234; r.logStartOffset = 5;
    r.assignedBase = 42;
    return r;
}
}

TEST(LeaseProtocolRegression, ProduceLeaseTrailerRegression) {
    Bytes verdicts(16);
    for (std::size_t i = 0; i < verdicts.size(); ++i) verdicts[i] = static_cast<uint8_t>(i + 1);
    for (const uint16_t lc : {uint16_t{1}, uint16_t{2048}}) {
        Bytes leases(16u * lc);
        for (std::size_t i = 0; i < leases.size(); ++i) leases[i] = static_cast<uint8_t>(i * 31 + 3);
        auto r = baseResponse();
        r.hasVerdicts = true; r.verdictCount = 2; r.verdictBytes = sp(verdicts);
        r.hasLease = true; r.leaseCount = lc; r.leaseBytes = sp(leases);
        Bytes buf(rc::ProduceResponse::kMaxEncodedSize);
        const std::size_t n = r.encodeFor(buf.data(), rc::kClientVersionLeases);
        EXPECT_EQ(n, r.encodedSize(rc::kClientVersionLeases));
        EXPECT_EQ(n, 54u + 2 + 16 + 3 + 16u * lc);
        EXPECT_EQ(n, rc::ProduceResponse::encodedSizeFor(18, true, 2, true, lc));
        EXPECT_LE(n, rc::ProduceResponse::kMaxEncodedSize);
        const auto d = rc::ProduceResponse::decode(buf.data(), n);
        ASSERT_TRUE(d.has_value());
        EXPECT_TRUE(d->hasVerdicts);
        EXPECT_EQ(d->verdictCount, 2u);
        EXPECT_TRUE(d->hasLease);
        EXPECT_EQ(d->leaseCount, lc);
        EXPECT_EQ(d->assignedBase, 42u);
        ASSERT_EQ(d->leaseBytes.size(), leases.size());
        EXPECT_EQ(std::memcmp(d->leaseBytes.data(), leases.data(), leases.size()), 0);
        // forEachLease reads little-endian {version i64, ttl u64}
        uint32_t seen = 0;
        d->forEachLease([&](uint16_t idx, int64_t ver, uint64_t ttl) {
            const uint8_t* e = leases.data() + 16u * idx;
            int64_t v = 0; uint64_t t = 0;
            for (int b = 0; b < 8; ++b) { v |= static_cast<int64_t>(static_cast<uint64_t>(e[b]) << (8 * b)); t |= static_cast<uint64_t>(e[8 + b]) << (8 * b); }
            EXPECT_EQ(ver, v);
            EXPECT_EQ(ttl, t);
            EXPECT_EQ(idx, seen);
            ++seen;
        });
        EXPECT_EQ(seen, lc);
        // a trailing byte, or a cut, is refused
        EXPECT_FALSE(rc::ProduceResponse::decode(buf.data(), n - 1).has_value());
        buf[n] = 0;
        EXPECT_FALSE(rc::ProduceResponse::decode(buf.data(), n + 1).has_value());
    }
    // (b) lease without verdicts: the verdict trailer is written with count 0
    {
        Bytes leases(16, 9);
        auto r = baseResponse();
        r.hasLease = true; r.leaseCount = 1; r.leaseBytes = sp(leases);
        Bytes buf(rc::ProduceResponse::kMaxEncodedSize);
        const std::size_t n = r.encodeFor(buf.data(), 18);
        EXPECT_EQ(n, 54u + 2 + 3 + 16);
        EXPECT_EQ(n, r.encodedSize(18));
        EXPECT_EQ(buf[54], 0); EXPECT_EQ(buf[55], 0);   // verdict count 0
        EXPECT_EQ(buf[56], 1);                          // lease marker
        const auto d = rc::ProduceResponse::decode(buf.data(), n);
        ASSERT_TRUE(d.has_value());
        EXPECT_TRUE(d->hasVerdicts);
        EXPECT_EQ(d->verdictCount, 0u);
        EXPECT_TRUE(d->hasLease);
        EXPECT_EQ(d->leaseCount, 1u);
        // (d) marker 2 is refused; a count beyond the remaining bytes is refused
        Bytes bad(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(n));
        bad[56] = 2;
        EXPECT_FALSE(rc::ProduceResponse::decode(bad.data(), bad.size()).has_value());
        bad[56] = 1; bad[57] = 2;   // count 2 but 1 entry present
        EXPECT_FALSE(rc::ProduceResponse::decode(bad.data(), bad.size()).has_value());
        bad[57] = 0; bad[58] = 0;   // count 0 but 16 stray bytes
        EXPECT_FALSE(rc::ProduceResponse::decode(bad.data(), bad.size()).has_value());
        Bytes big(bad.begin(), bad.begin() + 57);
        big.insert(big.end(), {0x01, 0x08});   // count 2049 with no data
        EXPECT_FALSE(rc::ProduceResponse::decode(big.data(), big.size()).has_value());
    }
    // (c) a session below 18 is never sent the trailer: byte-identical to hasLease=false
    for (const int32_t v : {0, 1, 2, 11, 16, 17}) {
        Bytes leases(16, 3);
        auto with = baseResponse();
        with.hasLease = true; with.leaseCount = 1; with.leaseBytes = sp(leases);
        const auto without = baseResponse();
        Bytes a(rc::ProduceResponse::kMaxEncodedSize), b(rc::ProduceResponse::kMaxEncodedSize);
        const std::size_t na = with.encodeFor(a.data(), v), nb = without.encodeFor(b.data(), v);
        ASSERT_EQ(na, nb) << v;
        EXPECT_EQ(std::memcmp(a.data(), b.data(), na), 0) << v;
        EXPECT_EQ(na, with.encodedSize(v)) << v;
    }
    // (e) pre-existing sizes unchanged
    EXPECT_EQ(rc::ProduceResponse::encodedSizeFor(16), 46u);
    EXPECT_EQ(rc::ProduceResponse::encodedSizeFor(1), 38u);
    EXPECT_EQ(rc::ProduceResponse::encodedSizeFor(17), 54u);
    EXPECT_EQ(rc::ProduceResponse::encodedSizeFor(18), 54u);
    EXPECT_EQ(rc::ProduceResponse::encodedSizeFor(17, true, 3), 54u + 2 + 24);
    EXPECT_EQ(rc::ProduceResponse::encodedSizeFor(16, true, 3), 46u);
    EXPECT_EQ(rc::ProduceResponse::encodedSizeFor(17, true, 3, true, 5), 54u + 2 + 24);
    EXPECT_EQ(rc::ProduceResponse::encodedSizeFor(18, false, 0, false, 0), 54u);
    EXPECT_EQ(rc::ProduceResponse::kMaxEncodedSize, 49211u);
}

// ---------------------------------------------------------------------------
// DefineTopic / TopicSchemaResponse lease blocks
// ---------------------------------------------------------------------------
namespace {
rc::DefineTopic leaseDefine(bool lease) {
    static const uint8_t desc[3] = {1, 2, 3};
    rc::DefineTopic m;
    m.sessionId = 1; m.correlationId = 2; m.topicName = "sessions"; m.partitionCount = 4; m.keyField = "id";
    m.validate = true; m.descriptorBlob = desc; m.descriptorLen = 3;
    m.hasExt = true; m.hasKeyFeatures = true; m.hasLease = lease;
    return m;
}
constexpr const char* kDefaultSpecHex =
    "3075000000000000e8030000000000000090cd792f08000000001000000101e0930400e8030000";
}

TEST(LeaseProtocolRegression, DefineTopicLeaseSpecRegression) {
    const auto plain = leaseDefine(false);
    const auto lease = leaseDefine(true);
    ASSERT_TRUE(plain.fits());
    ASSERT_TRUE(lease.fits());
    const Bytes pf = encodeVec(plain);
    const Bytes lf = encodeVec(lease);
    // the no-lease frame is the pre-change layout: its size is the formula without the lease block
    const std::size_t formula = 24 + rc::DefineTopic::BODY_FIXED + 2 + plain.topicName.size() + 2 + plain.keyField.size() +
                                4 + 3 + 1 + 2 + rc::topicConfigEncodedSize(plain.config) + rc::extEncodedSize(plain.ext) +
                                rp::kTopicKeyFeaturesWireLen;
    EXPECT_EQ(pf.size(), formula);
    EXPECT_EQ(lf.size(), pf.size() + 1 + 39);
    // the lease frame is the plain frame + marker + spec
    EXPECT_EQ(std::memcmp(lf.data(), pf.data(), pf.size()), 0);
    EXPECT_EQ(lf[pf.size()], 0x01);
    EXPECT_EQ(hexOf(lf.data() + pf.size() + 1, 39), kDefaultSpecHex);
    // round trip
    const auto d = rc::DefineTopic::decode(lf.data(), lf.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_TRUE(d->hasLease);
    EXPECT_TRUE(d->hasKeyFeatures);
    EXPECT_EQ(d->lease, rp::LeaseSpec{});
    EXPECT_EQ(d->topicName, "sessions");
    const auto dp = rc::DefineTopic::decode(pf.data(), pf.size());
    ASSERT_TRUE(dp.has_value());
    EXPECT_FALSE(dp->hasLease);
    // a non-default spec round-trips
    auto custom = lease;
    custom.lease.ttlMs = 5555; custom.lease.maxKeys = 77; custom.lease.checkpoint = 0; custom.lease.checkpointRate = 9;
    const Bytes cf = encodeVec(custom);
    const auto cd = rc::DefineTopic::decode(cf.data(), cf.size());
    ASSERT_TRUE(cd.has_value());
    EXPECT_EQ(cd->lease, custom.lease);
    // hasLease without hasKeyFeatures (or without hasExt): fits() false, encode 0
    {
        auto bad = lease;
        bad.hasKeyFeatures = false;
        Bytes scratch(512);
        EXPECT_FALSE(bad.fits());
        EXPECT_EQ(bad.encode(scratch.data()), 0u);
        bad.hasExt = false;
        EXPECT_FALSE(bad.fits());
        EXPECT_EQ(bad.encode(scratch.data()), 0u);
    }
    // marker != 1
    {
        Bytes f = lf;
        f[pf.size()] = 0x02;
        EXPECT_FALSE(rc::DefineTopic::decode(f.data(), f.size()).has_value());
        f[pf.size()] = 0x00;
        EXPECT_FALSE(rc::DefineTopic::decode(f.data(), f.size()).has_value());
    }
    // 39 trailing bytes instead of 40, and 41
    {
        Bytes f(lf.begin(), lf.end() - 1);
        EXPECT_FALSE(rc::DefineTopic::decode(f.data(), f.size()).has_value());
        Bytes g = lf; g.push_back(0);
        EXPECT_FALSE(rc::DefineTopic::decode(g.data(), g.size()).has_value());
    }
    // a checkpoint byte of 2 still decodes (validateLeaseSpec refuses it, so the broker answers InvalidRequest)
    {
        Bytes f = lf;
        f[pf.size() + 1 + 30] = 2;
        const auto cd2 = rc::DefineTopic::decode(f.data(), f.size());
        ASSERT_TRUE(cd2.has_value());
        EXPECT_EQ(cd2->lease.checkpoint, 2);
        EXPECT_TRUE(rp::validateLeaseSpec(cd2->lease).has_value());
    }
    // decode does NOT range-check (validateLeaseSpec is the broker's): an out-of-range spec still decodes
    {
        auto oor = lease;
        oor.lease.maxKeys = 0;
        const Bytes f = encodeVec(oor);
        const auto od = rc::DefineTopic::decode(f.data(), f.size());
        ASSERT_TRUE(od.has_value());
        EXPECT_TRUE(rp::validateLeaseSpec(od->lease).has_value());
    }
}

TEST(LeaseProtocolRegression, SchemaLeaseTrailerRegression) {
    const uint8_t desc[2] = {9, 8};
    rc::TopicSchemaResponse m;
    m.sessionId = 1; m.correlationId = 2; m.topicId = 5; m.version = 3; m.descriptorBlob = desc; m.descriptorLen = 2;
    m.partitionCount = 6; m.hasKeyFeatures = true;
    ASSERT_TRUE(m.fits());
    const Bytes noLease = encodeVec(m);
    {
        const auto d = rc::TopicSchemaResponse::decode(noLease.data(), noLease.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_FALSE(d->hasLease);
        EXPECT_TRUE(d->hasKeyFeatures);
        EXPECT_EQ(d->partitionCount, 6);
    }
    auto withLease = m;
    withLease.hasLease = true;
    withLease.lease.ttlMs = 4242;
    withLease.leaseViewTopicId = 77;
    const Bytes lf = encodeVec(withLease);
    EXPECT_EQ(lf.size(), noLease.size() + 44);   // marker + 39-byte spec + i32 lease-view topic id (Task 14a)
    EXPECT_EQ(std::memcmp(lf.data(), noLease.data(), noLease.size()), 0);
    EXPECT_EQ(lf[noLease.size()], 1);
    {
        const auto d = rc::TopicSchemaResponse::decode(lf.data(), lf.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_TRUE(d->hasLease);
        EXPECT_EQ(d->lease, withLease.lease);
        EXPECT_EQ(d->leaseViewTopicId, 77);
    }
    // a 40-byte lease block (no id) decodes with -1
    {
        const auto d = rc::TopicSchemaResponse::decode(lf.data(), lf.size() - 4);
        ASSERT_TRUE(d.has_value());
        EXPECT_TRUE(d->hasLease);
        EXPECT_EQ(d->leaseViewTopicId, -1);
    }
    // remainders of 0..39 bytes after the key block decode as no lease block
    for (std::size_t extra = 0; extra < 40; ++extra) {
        Bytes f = noLease;
        f.insert(f.end(), extra, 1);
        const auto d = rc::TopicSchemaResponse::decode(f.data(), f.size());
        ASSERT_TRUE(d.has_value()) << extra;
        EXPECT_FALSE(d->hasLease) << extra;
    }
    // marker 2 with 40 bytes; a checkpoint byte of 2
    {
        Bytes f = lf;
        f[noLease.size()] = 2;
        EXPECT_FALSE(rc::TopicSchemaResponse::decode(f.data(), f.size()).has_value());
        Bytes g = lf;
        g[noLease.size() + 1 + 30] = 2;   // decode checks the length only; validateLeaseSpec refuses the byte (M3)
        const auto gd = rc::TopicSchemaResponse::decode(g.data(), g.size());
        ASSERT_TRUE(gd.has_value());
        EXPECT_EQ(gd->lease.checkpoint, 2);
        EXPECT_TRUE(rp::validateLeaseSpec(gd->lease).has_value());
    }
    // hasLease without hasKeyFeatures is refused at encode
    {
        auto bad = withLease;
        bad.hasKeyFeatures = false;
        Bytes scratch(256);
        EXPECT_FALSE(bad.fits());
        EXPECT_EQ(bad.encode(scratch.data()), 0u);
    }
}

// ---------------------------------------------------------------------------
// LeaseSpec
// ---------------------------------------------------------------------------
TEST(LeaseProtocolRegression, LeaseSpecValidateRegression) {
    using S = rp::LeaseSpec;
    EXPECT_FALSE(rp::validateLeaseSpec(S{}).has_value());
    const auto bad = [](S s) { return rp::validateLeaseSpec(s).has_value(); };
    const auto with = [](auto set) { S s; set(s); return s; };

    EXPECT_TRUE(bad(with([](S& s) { s.minTtlMs = 999; })));
    EXPECT_FALSE(bad(with([](S& s) { s.minTtlMs = 1000; })));
    EXPECT_FALSE(bad(with([](S& s) { s.maxTtlMs = 9'000'000'000'000; })));
    EXPECT_TRUE(bad(with([](S& s) { s.maxTtlMs = 9'000'000'000'001; })));
    EXPECT_TRUE(bad(with([](S& s) { s.minTtlMs = 40'000; })));                                    // min > ttl and max still ok
    EXPECT_TRUE(bad(with([](S& s) { s.minTtlMs = 5000; s.maxTtlMs = 4000; s.ttlMs = 4500; }))); // min > max
    EXPECT_TRUE(bad(with([](S& s) { s.ttlMs = 999; })));                                        // below min
    EXPECT_FALSE(bad(with([](S& s) { s.ttlMs = 1000; })));
    EXPECT_TRUE(bad(with([](S& s) { s.maxTtlMs = 60'000; s.ttlMs = 60'001; })));                // above max
    EXPECT_FALSE(bad(with([](S& s) { s.maxTtlMs = 60'000; s.ttlMs = 60'000; })));
    for (const auto [v, badv] : {std::pair<uint32_t, bool>{0, true}, {1, false}, {16'777'216, false}, {16'777'217, true}})
        EXPECT_EQ(bad(with([&](S& s) { s.maxKeys = v; })), badv) << v;
    for (const auto [v, badv] : {std::pair<uint32_t, bool>{999, true}, {1000, false}, {86'400'000, false}, {86'400'001, true}})
        EXPECT_EQ(bad(with([&](S& s) { s.checkpointIntervalMs = v; })), badv) << v;
    for (const auto [v, badv] : {std::pair<uint32_t, bool>{0, true}, {1, false}, {100'000, false}, {100'001, true}})
        EXPECT_EQ(bad(with([&](S& s) { s.checkpointRate = v; })), badv) << v;
    for (const auto [v, badv] : {std::pair<uint16_t, bool>{0, true}, {1, false}, {4096, false}, {4097, true}})
        EXPECT_EQ(bad(with([&](S& s) { s.expiryBatch = v; })), badv) << v;
    EXPECT_FALSE(bad(with([](S& s) { s.checkpoint = 0; })));
    EXPECT_TRUE(bad(with([](S& s) { s.checkpoint = 2; })));
    // each failure carries a non-empty static message naming a lease key
    const auto msg = rp::validateLeaseSpec(with([](S& s) { s.minTtlMs = 999; }));
    ASSERT_TRUE(msg.has_value());
    EXPECT_NE(msg->find("lease.min_ttl"), std::string_view::npos);

    // encode / decode
    S s;
    s.ttlMs = 123456789012; s.minTtlMs = 2000; s.maxTtlMs = 8'000'000'000'000; s.maxKeys = 4242; s.expiryBatch = 77;
    s.checkpoint = 0; s.checkpointIntervalMs = 12345; s.checkpointRate = 678;
    uint8_t buf[rp::kLeaseSpecBytes];
    rp::encodeLeaseSpec(s, buf);
    S out;
    ASSERT_TRUE(rp::decodeLeaseSpec(buf, sizeof buf, out));
    EXPECT_EQ(out, s);
    EXPECT_EQ(hexOf(buf, 39).size(), 78u);
    EXPECT_FALSE(rp::decodeLeaseSpec(buf, 38, out));
    uint8_t cp[rp::kLeaseSpecBytes];
    std::memcpy(cp, buf, sizeof cp);
    cp[30] = 2;
    ASSERT_TRUE(rp::decodeLeaseSpec(cp, sizeof cp, out));   // decode checks the length only (M3)
    EXPECT_EQ(out.checkpoint, 2);
    EXPECT_TRUE(rp::validateLeaseSpec(out).has_value());
    uint8_t def[rp::kLeaseSpecBytes];
    rp::encodeLeaseSpec(S{}, def);
    EXPECT_EQ(hexOf(def, 39), kDefaultSpecHex);
}

// ---------------------------------------------------------------------------
// templates, codes, numbers
// ---------------------------------------------------------------------------
TEST(LeaseProtocolRegression, TemplateDispatchRegression) {
    for (uint16_t t = 96; t <= 101; ++t) EXPECT_TRUE(rc::isKnownTemplate(t)) << t;
    for (const uint16_t t : {uint16_t{93}, uint16_t{94}, uint16_t{95}, uint16_t{102}, uint16_t{111}, uint16_t{112}})
        EXPECT_FALSE(rc::isKnownTemplate(t)) << t;
    std::array<uint8_t, 24> hdr{};
    rc::MessageHeader h;
    h.templateId = 102;
    h.encode(hdr.data());
    EXPECT_EQ(rc::checkFrame(hdr.data(), hdr.size()).status, rc::FrameStatus::UnknownTemplate);
    h.templateId = 96;
    h.encode(hdr.data());
    EXPECT_EQ(rc::checkFrame(hdr.data(), hdr.size()).status, rc::FrameStatus::Ok);
    EXPECT_EQ(static_cast<int>(rc::TemplateId::LeaseKeepAlive), 96);
    EXPECT_EQ(static_cast<int>(rc::TemplateId::LeaseKeepAliveResponse), 97);
    EXPECT_EQ(static_cast<int>(rc::TemplateId::LeaseTimeToLive), 98);
    EXPECT_EQ(static_cast<int>(rc::TemplateId::LeaseTimeToLiveResponse), 99);
    EXPECT_EQ(static_cast<int>(rc::TemplateId::ScanLeases), 100);
    EXPECT_EQ(static_cast<int>(rc::TemplateId::ScanLeasesResponse), 101);
    EXPECT_EQ(rc::LeaseKeepAlive::TEMPLATE_ID, 96);
    EXPECT_EQ(rc::ScanLeasesResponse::TEMPLATE_ID, 101);
}

TEST(LeaseProtocolRegression, NumbersPinnedRegression) {
    EXPECT_EQ(static_cast<int>(rc::ErrorCode::LeaseNotFound), 64);
    EXPECT_EQ(static_cast<int>(rc::ErrorCode::LeaseLimitExceeded), 65);
    EXPECT_EQ(static_cast<int>(rc::ErrorCode::LeaseTtlTooLarge), 66);
    EXPECT_EQ(rc::kClientVersionLeases, 18);
    EXPECT_EQ(rc::kClientVersion, 18);
    EXPECT_EQ(rc::kBrokerVersionLeases, 11);
    EXPECT_EQ(rc::kBrokerVersion, 11);
    EXPECT_FALSE(rc::clientUnderstandsLeases(17));
    EXPECT_TRUE(rc::clientUnderstandsLeases(18));
    for (const auto ec : {rc::ErrorCode::LeaseNotFound, rc::ErrorCode::LeaseLimitExceeded, rc::ErrorCode::LeaseTtlTooLarge}) {
        EXPECT_EQ(rc::produceErrorForClient(ec, 17), rc::ErrorCode::InvalidRequest);
        EXPECT_EQ(rc::produceErrorForClient(ec, 0), rc::ErrorCode::InvalidRequest);
        EXPECT_EQ(rc::produceErrorForClient(ec, 18), ec);
    }
    EXPECT_EQ(rc::produceErrorForClient(rc::ErrorCode::ViewUniqueInvalid, 17), rc::ErrorCode::ViewUniqueInvalid);
    EXPECT_EQ(rp::kLeaseSpecBytes, 39u);
    EXPECT_EQ(rp::kMaxLeaseKeyBytes, 128u);
    EXPECT_EQ(rr::kLeaseVersionValueOffset, 4u);
    EXPECT_EQ(rr::kLeaseTtlValueOffset, 12u);
    EXPECT_EQ(rr::kLeaseHolderValueOffset, 20u);
    EXPECT_EQ(rr::kLeaseRemainingValueOffset, 28u);
    EXPECT_EQ(rr::kLeaseSystemBytes, 32u);
    EXPECT_EQ(rr::kAttrLeaseOpMask, 0x0C);
    EXPECT_EQ(rr::kAttrLeaseOpShift, 2);
    EXPECT_EQ(rc::kLeaseKeepAliveMaxEntries, 256u);
    EXPECT_EQ(rc::kScanLeasesMaxRows, 1024u);
    EXPECT_EQ(rc::kScanLeasesMaxPrefixBytes, 127u);
}

// ---------------------------------------------------------------------------
// review fixes: the largest ack, the checkpoint byte, truncation at every offset, v17 identity, pinned bounds
// ---------------------------------------------------------------------------
TEST(LeaseProtocolRegression, MaxLeaseAckFitsOneAeronMessageRegression) {
    static_assert(rr::kMaxLeaseBatchRecords == 2048);
    EXPECT_EQ(rc::ProduceResponse::kMaxEncodedSize, 49211u);
    EXPECT_LE(rc::ProduceResponse::kMaxEncodedSize + rc::kCryptoOverhead, 65536u);   // term 512 KiB / 8
    // the largest lease ack encodes within kMaxEncodedSize and decodes back
    Bytes verdicts(8u * 2048, 5), leases(16u * 2048, 7);
    auto r = baseResponse();
    r.hasVerdicts = true; r.verdictCount = 2048; r.verdictBytes = sp(verdicts);
    r.hasLease = true; r.leaseCount = 2048; r.leaseBytes = sp(leases);
    Bytes buf(rc::ProduceResponse::kMaxEncodedSize);
    const std::size_t n = r.encodeFor(buf.data(), rc::kClientVersionLeases);
    EXPECT_EQ(n, rc::ProduceResponse::kMaxEncodedSize);
    const auto d = rc::ProduceResponse::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->leaseCount, 2048u);
    // 2049 lease entries, or 2049 verdicts before a lease trailer, are refused by decode
    for (const bool leaseOver : {true, false}) {
        Bytes f(54 + 2 + 8u * 2049 + 3 + 16u * 2049, 0);
        const uint16_t vc = leaseOver ? 0 : 2049, lc = leaseOver ? 2049 : 0;
        const std::size_t len = 54 + 2 + 8u * vc + 3 + 16u * lc;
        std::memcpy(f.data(), buf.data(), 54);
        f[54] = static_cast<uint8_t>(vc & 0xff); f[55] = static_cast<uint8_t>(vc >> 8);
        const std::size_t lp = 56 + 8u * vc;
        f[lp] = 1; f[lp + 1] = static_cast<uint8_t>(lc & 0xff); f[lp + 2] = static_cast<uint8_t>(lc >> 8);
        EXPECT_FALSE(rc::ProduceResponse::decode(f.data(), len).has_value()) << leaseOver;
    }
}

TEST(LeaseProtocolRegression, CheckpointByteTwoDecodesAndValidateRefusesRegression) {
    rp::LeaseSpec s;
    uint8_t buf[rp::kLeaseSpecBytes];
    rp::encodeLeaseSpec(s, buf);
    buf[30] = 2;
    rp::LeaseSpec out;
    ASSERT_TRUE(rp::decodeLeaseSpec(buf, sizeof buf, out));
    EXPECT_EQ(out.checkpoint, 2);
    const auto why = rp::validateLeaseSpec(out);
    ASSERT_TRUE(why.has_value());
    EXPECT_EQ(*why, "lease.checkpoint must be 0 or 1");
    EXPECT_FALSE(rp::decodeLeaseSpec(buf, rp::kLeaseSpecBytes - 1, out));
}

TEST(LeaseProtocolRegression, LeaseTrailersTruncatedAtEveryOffsetRegression) {
    // ProduceResponse (verdicts + lease): every cut from the start of the verdict trailer (54) to full-1.  Cut at 54
    // is the v17 54-byte frame = a response with no trailer (decodes); every other cut is refused (a trailer is
    // exact-length).
    {
        Bytes verdicts(16, 1), leases(32, 2);
        auto r = baseResponse();
        r.hasVerdicts = true; r.verdictCount = 2; r.verdictBytes = sp(verdicts);
        r.hasLease = true; r.leaseCount = 2; r.leaseBytes = sp(leases);
        Bytes buf(rc::ProduceResponse::kMaxEncodedSize);
        const std::size_t n = r.encodeFor(buf.data(), 18);
        ASSERT_EQ(n, 54u + 2 + 16 + 3 + 32);
        for (std::size_t cut = 54; cut < n; ++cut) {
            const auto d = rc::ProduceResponse::decode(buf.data(), cut);
            if (cut == 54) {
                ASSERT_TRUE(d.has_value()) << cut;
                EXPECT_FALSE(d->hasVerdicts);
                EXPECT_FALSE(d->hasLease);
            } else if (cut == 54u + 2 + 16) {   // exactly the verdict trailer: a verdict-only ack
                ASSERT_TRUE(d.has_value()) << cut;
                EXPECT_TRUE(d->hasVerdicts);
                EXPECT_FALSE(d->hasLease);
            } else {
                EXPECT_FALSE(d.has_value()) << cut;
            }
        }
    }
    // DefineTopic with a lease: cuts inside [marker + 39-byte spec] are refused (the frame must end exactly at the
    // lease block, or before it where the format defines "no lease": cut == start of the block is the no-lease frame).
    {
        const auto lease = leaseDefine(true);
        const Bytes lf = encodeVec(lease);
        const Bytes pf = encodeVec(leaseDefine(false));
        for (std::size_t cut = pf.size(); cut < lf.size(); ++cut) {
            const auto d = rc::DefineTopic::decode(lf.data(), cut);
            if (cut == pf.size()) {
                ASSERT_TRUE(d.has_value()) << cut;
                EXPECT_FALSE(d->hasLease);
            } else {
                EXPECT_FALSE(d.has_value()) << cut;
            }
        }
    }
    // TopicSchemaResponse with a lease: a remainder under 40 bytes is "no lease" (decodes); only the full 40 is a lease.
    {
        const uint8_t desc[2] = {9, 8};
        rc::TopicSchemaResponse m;
        m.sessionId = 1; m.correlationId = 2; m.topicId = 5; m.version = 3; m.descriptorBlob = desc; m.descriptorLen = 2;
        m.partitionCount = 6; m.hasKeyFeatures = true; m.hasLease = true;
        const Bytes lf = encodeVec(m);
        const std::size_t start = lf.size() - 44;
        for (std::size_t cut = start; cut < lf.size(); ++cut) {
            const auto d = rc::TopicSchemaResponse::decode(lf.data(), cut);
            ASSERT_TRUE(d.has_value()) << cut;
            EXPECT_EQ(d->hasLease, cut >= start + 40) << cut;
        }
        const auto full = rc::TopicSchemaResponse::decode(lf.data(), lf.size());
        ASSERT_TRUE(full.has_value());
        EXPECT_TRUE(full->hasLease);
    }
}

TEST(LeaseProtocolRegression, V17FrameWithLeaseInputIsByteIdenticalRegression) {
    Bytes verdicts(16, 4), leases(32, 6);
    auto both = baseResponse();
    both.hasVerdicts = true; both.verdictCount = 2; both.verdictBytes = sp(verdicts);
    both.hasLease = true; both.leaseCount = 2; both.leaseBytes = sp(leases);
    auto only = baseResponse();
    only.hasVerdicts = true; only.verdictCount = 2; only.verdictBytes = sp(verdicts);
    Bytes a(rc::ProduceResponse::kMaxEncodedSize), b(rc::ProduceResponse::kMaxEncodedSize);
    const std::size_t na = both.encodeFor(a.data(), 17), nb = only.encodeFor(b.data(), 17);
    ASSERT_EQ(na, nb);
    EXPECT_EQ(na, 54u + 2 + 16);
    EXPECT_EQ(std::memcmp(a.data(), b.data(), na), 0);
}

TEST(LeaseProtocolRegression, LeaseBoundsPinnedRegression) {
    static_assert(rp::kLeaseMinTtlFloorMs == 1000);
    static_assert(rp::kLeaseMaxTtlMs == 9'000'000'000'000);
    static_assert(rr::kMaxLeaseBatchRecords == 2048);
    EXPECT_EQ(rp::kLeaseMinTtlFloorMs, 1000u);
    EXPECT_EQ(rp::kLeaseMaxTtlMs, 9'000'000'000'000u);
    EXPECT_EQ(rr::kMaxLeaseBatchRecords, 2048u);
    // MetadataFeature::kLeases == 6 is pinned in broker/libs/node/tests/node/CheckedWritesFeatureLevelTest.cpp
}
