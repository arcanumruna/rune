#include <gtest/gtest.h>
#include <rune/protocol/Varint.h>
#include <climits>
#include <vector>
#include <cstdint>
using namespace rune::protocol;

TEST(Varint, RoundTripLong) {
    for (int64_t v : {0LL, 1LL, -1LL, 127LL, -128LL, 300LL, -300LL,
                      9223372036854775807LL, -9223372036854775807LL - 1}) {
        uint8_t buf[10]; size_t n = writeVarlong(v, buf);
        size_t pos = 0; int64_t got = readVarlong(buf, pos);
        EXPECT_EQ(got, v);
        EXPECT_EQ(pos, n);
    }
}

// ---------------------------------------------------------------------------
// Edge-case tests added for comprehensive coverage
// ---------------------------------------------------------------------------

// --- int32 boundary round-trips ---

TEST(Varint, RoundTripInt32Boundaries) {
    struct Case { int32_t v; const char* label; };
    Case cases[] = {
        {0,          "zero"},
        {1,          "one"},
        {-1,         "minus one"},
        {63,         "63"},
        {64,         "64"},
        {127,        "127"},
        {128,        "128"},
        {8191,       "8191"},
        {8192,       "8192"},
        {INT32_MAX,  "INT32_MAX"},
        {INT32_MIN,  "INT32_MIN"},
        {-63,        "-63"},
        {-64,        "-64"},
        {-128,       "-128"},
        {-129,       "-129"},
    };
    for (auto& c : cases) {
        uint8_t buf[10];
        size_t n = writeVarint(c.v, buf);
        size_t pos = 0;
        int32_t got = readVarint(buf, pos);
        EXPECT_EQ(got, c.v) << c.label;
        EXPECT_EQ(pos, n)   << c.label << ": pos != writeVarint return";
    }
}

// --- Encoded byte-length transitions for varlong (zigzag 7-bit boundaries) ---

TEST(Varint, VarlongEncodedLengthTransitions) {
    // After zigzag encoding, positive value v encodes to ceil((bits+1)/7) bytes.
    // Transition points (encoded zigzag value crosses each 7-bit boundary):
    //   zigzag(v>=0) = 2*v => boundary zigzag values are 0x7F, 0x3FFF, 0x1FFFFF, ...
    //   so boundaries for positive int64:
    //     1 byte: v in [0, 63]          (zigzag 0..126 < 0x80)
    //     2 bytes: v in [64, 8191]      (zigzag 128..16382 < 0x4000)
    //     3 bytes: v in [8192, 1048575] ...
    struct Case { int64_t v; size_t expected_bytes; };
    Case cases[] = {
        {0,                    1},
        {63,                   1},
        {64,                   2},
        {8191,                 2},
        {8192,                 3},
        {1048575,              3},
        {1048576,              4},
        {134217727LL,          4},
        {134217728LL,          5},
        // negative values: zigzag(v<0) = 2*(-v)-1 (same bit count as positive magnitude)
        {-1,                   1},   // zigzag = 1
        {-64,                  1},   // zigzag = 127
        {-65,                  2},   // zigzag = 129
        {-8192,                2},   // zigzag = 16383
        {-8193,                3},   // zigzag = 16385
    };
    for (auto& c : cases) {
        uint8_t buf[10];
        size_t n = writeVarlong(c.v, buf);
        EXPECT_EQ(n, c.expected_bytes) << "v=" << c.v;
        // Verify round-trip for all transition values.
        size_t pos = 0;
        EXPECT_EQ(readVarlong(buf, pos), c.v) << "v=" << c.v;
        EXPECT_EQ(pos, n) << "v=" << c.v;
    }
}

// --- Multi-value stream: encode several values, decode in order ---

TEST(Varint, MultiValueStreamVarint) {
    int32_t values[] = {0, 1, -1, 127, -128, 8192, INT32_MAX, INT32_MIN, 42};
    uint8_t buf[100];
    size_t total = 0;
    size_t write_sizes[9];
    for (int i = 0; i < 9; ++i) {
        size_t n = writeVarint(values[i], buf + total);
        write_sizes[i] = n;
        total += n;
    }

    size_t pos = 0;
    for (int i = 0; i < 9; ++i) {
        size_t before = pos;
        int32_t got = readVarint(buf, pos);
        EXPECT_EQ(got, values[i]) << "index " << i;
        EXPECT_EQ(pos - before, write_sizes[i]) << "advance mismatch at index " << i;
    }
    EXPECT_EQ(pos, total);
}

TEST(Varint, MultiValueStreamVarlong) {
    int64_t values[] = {
        0LL, 1LL, -1LL,
        63LL, 64LL, 8191LL, 8192LL,
        INT64_MAX, INT64_MIN,
        -42LL, 1000000LL
    };
    constexpr int N = 11;
    uint8_t buf[120];
    size_t total = 0;
    size_t write_sizes[N];
    for (int i = 0; i < N; ++i) {
        size_t n = writeVarlong(values[i], buf + total);
        write_sizes[i] = n;
        total += n;
    }

    size_t pos = 0;
    for (int i = 0; i < N; ++i) {
        size_t before = pos;
        int64_t got = readVarlong(buf, pos);
        EXPECT_EQ(got, values[i]) << "index " << i;
        EXPECT_EQ(pos - before, write_sizes[i]) << "advance mismatch at index " << i;
    }
    EXPECT_EQ(pos, total);
}

// --- pos advancement equals writeVarlong return for every test value ---

TEST(Varint, PosAdvancementEqualsWriteReturn) {
    // For every value in a wide set, verify readVarlong pos advance == writeVarlong return.
    std::vector<int64_t> vals = {
        INT64_MIN, -1LL << 48, -1LL << 35, -1LL << 28,
        -1LL << 21, -1LL << 14, -1LL << 7, -1LL,
        0LL, 1LL, 1LL << 7, 1LL << 14, 1LL << 21,
        1LL << 28, 1LL << 35, 1LL << 42, 1LL << 48,
        INT64_MAX
    };
    for (int64_t v : vals) {
        uint8_t buf[10];
        size_t written = writeVarlong(v, buf);
        size_t pos = 0;
        int64_t got = readVarlong(buf, pos);
        EXPECT_EQ(got, v) << "v=" << v;
        EXPECT_EQ(pos, written) << "v=" << v;
    }
}

// --- INT64 extremes ---

TEST(Varint, Int64Extremes) {
    uint8_t buf[10];
    {
        size_t n = writeVarlong(INT64_MAX, buf);
        EXPECT_EQ(n, 10u); // INT64_MAX requires 10 bytes (zigzag = 2*INT64_MAX)
        size_t pos = 0;
        EXPECT_EQ(readVarlong(buf, pos), INT64_MAX);
        EXPECT_EQ(pos, 10u);
    }
    {
        size_t n = writeVarlong(INT64_MIN, buf);
        EXPECT_EQ(n, 10u); // INT64_MIN also requires 10 bytes
        size_t pos = 0;
        EXPECT_EQ(readVarlong(buf, pos), INT64_MIN);
        EXPECT_EQ(pos, 10u);
    }
}

// --- varint is a thin wrapper over varlong for int32 ---

TEST(Varint, VarintInt32MaxMinUseVarlongEncoding) {
    uint8_t buf32[10], buf64[10];
    size_t n32 = writeVarint(INT32_MAX, buf32);
    size_t n64 = writeVarlong(static_cast<int64_t>(INT32_MAX), buf64);
    EXPECT_EQ(n32, n64);
    for (size_t i = 0; i < n32; ++i)
        EXPECT_EQ(buf32[i], buf64[i]) << "byte " << i;

    size_t n32m = writeVarint(INT32_MIN, buf32);
    size_t n64m = writeVarlong(static_cast<int64_t>(INT32_MIN), buf64);
    EXPECT_EQ(n32m, n64m);
    for (size_t i = 0; i < n32m; ++i)
        EXPECT_EQ(buf32[i], buf64[i]) << "byte " << i;
}
