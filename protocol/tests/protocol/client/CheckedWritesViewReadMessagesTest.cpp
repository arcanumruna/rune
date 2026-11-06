#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <gtest/gtest.h>
#include <cstdio>
#include <string>
#include <vector>

namespace pc = rune::protocol::client;

namespace {

constexpr int64_t kSession = 0x1122334455667788LL;
constexpr int64_t kCorr    = 0x0A0B0C0D0E0F1011LL;

std::vector<uint8_t> bytes(std::size_t n, uint8_t first) {
    std::vector<uint8_t> v(n);
    for (std::size_t i = 0; i < n; ++i) v[i] = static_cast<uint8_t>(first + i);
    return v;
}

template <class T>
std::vector<uint8_t> encodeVec(const T& m) {
    std::vector<uint8_t> b(m.encodedSize());
    EXPECT_EQ(m.encode(b.data()), b.size());
    return b;
}

std::vector<uint8_t> readVector(const char* name) {
    const std::string path = std::string(RUNE_CONFORMANCE_DIR) + "/vectors/" + name;
    FILE* f = std::fopen(path.c_str(), "rb");
    EXPECT_NE(f, nullptr) << path;
    if (!f) return {};
    std::vector<uint8_t> b;
    uint8_t chunk[256];
    std::size_t got;
    while ((got = std::fread(chunk, 1, sizeof chunk, f)) > 0) b.insert(b.end(), chunk, chunk + got);
    std::fclose(f);
    return b;
}

pc::GetViewRowK makeGet(const std::vector<uint8_t>& key) {
    pc::GetViewRowK m;
    m.sessionId = kSession; m.correlationId = kCorr;
    m.viewTopicId = 7; m.partition = 2; m.requestedFingerprint = 0x0102030405060708ULL; m.minOffset = 1000;
    m.key = key;
    return m;
}

pc::ScanViewRows makeScan(const std::vector<uint8_t>& prefix, const std::vector<uint8_t>& token) {
    pc::ScanViewRows m;
    m.sessionId = kSession; m.correlationId = kCorr;
    m.viewTopicId = 7; m.limit = 100; m.requestedFingerprint = 0x0102030405060708ULL; m.minOffset = -1;
    m.prefix = prefix; m.resumeToken = token;
    return m;
}

std::vector<uint8_t> makeRows(int count) {
    std::vector<uint8_t> rows;
    for (int r = 0; r < count; ++r) {
        rows.insert(rows.end(), {32, 0, 0, 0});
        const auto row = bytes(32, static_cast<uint8_t>(0x30 * (r + 1)));
        rows.insert(rows.end(), row.begin(), row.end());
    }
    return rows;
}

bool sameBytes(std::span<const uint8_t> a, const std::vector<uint8_t>& b) {
    return std::vector<uint8_t>(a.begin(), a.end()) == b;
}

}  // namespace

TEST(CheckedWritesViewReadMessages, GetViewRowKRoundTrip) {
    for (const std::size_t keyLen : {1u, 16u, 74u, 128u}) {
        const auto key = bytes(keyLen, 0x40);
        const auto m = makeGet(key);
        EXPECT_EQ(m.encodedSize(), 52 + keyLen);
        const auto wire = encodeVec(m);
        const auto d = pc::GetViewRowK::decode(wire.data(), wire.size());
        ASSERT_TRUE(d);
        EXPECT_EQ(d->sessionId, kSession);
        EXPECT_EQ(d->correlationId, kCorr);
        EXPECT_EQ(d->viewTopicId, 7);
        EXPECT_EQ(d->partition, 2);
        EXPECT_EQ(d->requestedFingerprint, 0x0102030405060708ULL);
        EXPECT_EQ(d->minOffset, 1000);
        EXPECT_TRUE(sameBytes(d->key, key));
    }
}

TEST(CheckedWritesViewReadMessages, ScanRequestRoundTrip) {
    const auto prefix = bytes(5, 'a');
    for (const std::size_t tokenLen : {0u, 12u}) {
        const auto token = bytes(tokenLen, 0xA0);
        const auto m = makeScan(prefix, token);
        const auto wire = encodeVec(m);
        EXPECT_EQ(wire.size(), 24u + 24u + 4u + 5u + 4u + tokenLen);
        const auto d = pc::ScanViewRows::decode(wire.data(), wire.size());
        ASSERT_TRUE(d);
        EXPECT_EQ(d->sessionId, kSession);
        EXPECT_EQ(d->correlationId, kCorr);
        EXPECT_EQ(d->viewTopicId, 7);
        EXPECT_EQ(d->limit, 100);
        EXPECT_EQ(d->requestedFingerprint, 0x0102030405060708ULL);
        EXPECT_EQ(d->minOffset, -1);
        EXPECT_TRUE(sameBytes(d->prefix, prefix));
        EXPECT_TRUE(sameBytes(d->resumeToken, token));
    }
}

TEST(CheckedWritesViewReadMessages, ScanResponseRoundTrip) {
    const auto token = bytes(12, 0xB0);
    const auto rows = makeRows(3);
    pc::ScanViewRowsResponse m;
    m.sessionId = kSession; m.correlationId = kCorr;
    m.status = pc::ViewRowResponse::kOk; m.rowCount = 3; m.nextToken = token; m.rowsBytes = rows;
    auto wire = encodeVec(m);
    auto d = pc::ScanViewRowsResponse::decode(wire.data(), wire.size());
    ASSERT_TRUE(d);
    EXPECT_EQ(d->status, pc::ViewRowResponse::kOk);
    EXPECT_EQ(d->rowCount, 3);
    EXPECT_TRUE(sameBytes(d->nextToken, token));
    int seen = 0;
    EXPECT_TRUE(d->forEachRow([&](std::span<const uint8_t> row) {
        EXPECT_TRUE(sameBytes(row, bytes(32, static_cast<uint8_t>(0x30 * (seen + 1)))));
        ++seen;
    }));
    EXPECT_EQ(seen, 3);

    pc::ScanViewRowsResponse empty;
    empty.sessionId = kSession; empty.correlationId = kCorr;
    wire = encodeVec(empty);
    EXPECT_EQ(wire.size(), 24u + 4u + 4u + 4u);
    d = pc::ScanViewRowsResponse::decode(wire.data(), wire.size());
    ASSERT_TRUE(d);
    EXPECT_EQ(d->rowCount, 0);
    EXPECT_TRUE(d->nextToken.empty());
    EXPECT_TRUE(d->rowsBytes.empty());
    EXPECT_TRUE(d->forEachRow([](std::span<const uint8_t>) { ADD_FAILURE(); }));
}

TEST(CheckedWritesViewReadMessagesRegression, BoundsAreRefusedRegression) {
    for (const std::size_t keyLen : {0u, 129u}) {
        const auto key = bytes(keyLen, 1);
        const auto wire = encodeVec(makeGet(key));
        EXPECT_FALSE(pc::GetViewRowK::decode(wire.data(), wire.size())) << keyLen;
    }
    for (const std::size_t prefixLen : {0u, 128u}) {
        const auto wire = encodeVec(makeScan(bytes(prefixLen, 1), {}));
        EXPECT_FALSE(pc::ScanViewRows::decode(wire.data(), wire.size())) << prefixLen;
    }
    for (const std::size_t tokenLen : {1u, 11u, 13u, 129u}) {
        const auto scan = encodeVec(makeScan(bytes(5, 'a'), bytes(tokenLen, 1)));
        EXPECT_FALSE(pc::ScanViewRows::decode(scan.data(), scan.size())) << tokenLen;
        pc::ScanViewRowsResponse r;
        const auto token = bytes(tokenLen, 1);
        r.nextToken = token;
        const auto resp = encodeVec(r);
        EXPECT_FALSE(pc::ScanViewRowsResponse::decode(resp.data(), resp.size())) << tokenLen;
    }

    auto scan = encodeVec(makeScan(bytes(5, 'a'), bytes(12, 1)));
    scan[24 + 6] = 1;   // reserved
    EXPECT_FALSE(pc::ScanViewRows::decode(scan.data(), scan.size()));

    const auto rows = makeRows(3);
    pc::ScanViewRowsResponse r;
    const auto token = bytes(12, 1);
    r.rowCount = 3; r.nextToken = token; r.rowsBytes = rows;
    const std::vector<std::vector<uint8_t>> frames = {
        encodeVec(makeGet(bytes(16, 0x40))), encodeVec(makeScan(bytes(5, 'a'), bytes(12, 1))), encodeVec(r)};
    for (std::size_t f = 0; f < frames.size(); ++f) {
        auto decodes = [f](const std::vector<uint8_t>& b) {
            switch (f) {
                case 0: return pc::GetViewRowK::decode(b.data(), b.size()).has_value();
                case 1: return pc::ScanViewRows::decode(b.data(), b.size()).has_value();
                default: return pc::ScanViewRowsResponse::decode(b.data(), b.size()).has_value();
            }
        };
        EXPECT_TRUE(decodes(frames[f])) << f;
        for (std::size_t len = 0; len < frames[f].size(); ++len) {
            EXPECT_FALSE(decodes({frames[f].begin(), frames[f].begin() + static_cast<std::ptrdiff_t>(len)})) << f << "/" << len;
        }
        auto extra = frames[f];
        extra.push_back(0);
        EXPECT_FALSE(decodes(extra)) << f;
    }

    // the last row's length overruns the rows region
    auto bad = rows;
    bad[2 * 36] = 33;
    pc::ScanViewRowsResponse b;
    b.rowCount = 3; b.rowsBytes = bad;
    EXPECT_FALSE(b.forEachRow([](std::span<const uint8_t>) {}));
    // rows region shorter or longer than rowCount rows
    b.rowCount = 4; b.rowsBytes = rows;
    EXPECT_FALSE(b.forEachRow([](std::span<const uint8_t>) {}));
    b.rowCount = 2;
    EXPECT_FALSE(b.forEachRow([](std::span<const uint8_t>) {}));
}

TEST(CheckedWritesViewReadMessages, KnownTemplates) {
    EXPECT_TRUE(pc::isKnownTemplate(88));
    EXPECT_TRUE(pc::isKnownTemplate(89));
    EXPECT_TRUE(pc::isKnownTemplate(90));
    EXPECT_FALSE(pc::isKnownTemplate(91));
}

TEST(CheckedWritesViewReadMessages, ConformanceVectors) {
    const auto getWire = readVector("get_view_row_k.bin");
    ASSERT_EQ(getWire.size(), 68u);
    const auto g = pc::GetViewRowK::decode(getWire.data(), getWire.size());
    ASSERT_TRUE(g);
    EXPECT_EQ(g->sessionId, kSession);
    EXPECT_EQ(g->correlationId, kCorr);
    EXPECT_EQ(g->viewTopicId, 7);
    EXPECT_EQ(g->partition, 2);
    EXPECT_EQ(g->requestedFingerprint, 0x0102030405060708ULL);
    EXPECT_EQ(g->minOffset, 1000);
    EXPECT_TRUE(sameBytes(g->key, bytes(16, 0x40)));
    EXPECT_EQ(encodeVec(*g), getWire);

    const auto scanWire = readVector("scan_view_rows.bin");
    ASSERT_EQ(scanWire.size(), 73u);
    const auto s = pc::ScanViewRows::decode(scanWire.data(), scanWire.size());
    ASSERT_TRUE(s);
    EXPECT_EQ(s->viewTopicId, 7);
    EXPECT_EQ(s->limit, 100);
    EXPECT_EQ(s->requestedFingerprint, 0x0102030405060708ULL);
    EXPECT_EQ(s->minOffset, -1);
    EXPECT_TRUE(sameBytes(s->prefix, std::vector<uint8_t>{'a', 'c', 'c', 't', '/'}));
    EXPECT_TRUE(sameBytes(s->resumeToken, bytes(12, 0xA0)));
    EXPECT_EQ(encodeVec(*s), scanWire);

    const auto respWire = readVector("scan_view_rows_response.bin");
    ASSERT_EQ(respWire.size(), 156u);
    const auto r = pc::ScanViewRowsResponse::decode(respWire.data(), respWire.size());
    ASSERT_TRUE(r);
    EXPECT_EQ(r->status, 0);
    EXPECT_EQ(r->rowCount, 3);
    EXPECT_TRUE(sameBytes(r->nextToken, bytes(12, 0xB0)));
    EXPECT_TRUE(sameBytes(r->rowsBytes, makeRows(3)));
    EXPECT_EQ(encodeVec(*r), respWire);
}

TEST(CheckedWritesViewReadMessages, CursorInvalidatedIsEight) {
    EXPECT_EQ(pc::ViewRowResponse::kCursorInvalidated, 8);
}
