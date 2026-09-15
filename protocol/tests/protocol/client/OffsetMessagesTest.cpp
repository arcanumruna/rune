// OffsetMessagesTest.cpp — CG-1 tests for offset commit/fetch codecs (templateIds 40–43).
//
// Coverage per message:
//   (1) encode→decode round-trip asserting all fields (incl. multi-element arrays,
//       var metadata fields with/without content, empty entries = fetch-all)
//   (2) truncation: decode(buf, buf.size()-1) → nullopt
//   (3) wrong-template/schema: decode → nullopt
// Plus: conformance vector for OffsetCommit (with metadata).

#include <gtest/gtest.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <array>
#include <climits>
#include <cstdint>
#include <cstring>
#include <vector>

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Test-local LE readers
// ---------------------------------------------------------------------------
static uint16_t leU16(const uint8_t* buf, int off) {
    return static_cast<uint16_t>(uint16_t(buf[off]) | (uint16_t(buf[off + 1]) << 8));
}
static int16_t  leS16(const uint8_t* buf, int off) {
    return static_cast<int16_t>(leU16(buf, off));
}
static int32_t  leS32(const uint8_t* buf, int off) {
    uint32_t v = uint32_t(buf[off])
               | (uint32_t(buf[off + 1]) << 8)
               | (uint32_t(buf[off + 2]) << 16)
               | (uint32_t(buf[off + 3]) << 24);
    return static_cast<int32_t>(v);
}
static int64_t  leS64(const uint8_t* buf, int off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(buf[off + i]) << (i * 8);
    return static_cast<int64_t>(v);
}

// Canonical field values
static constexpr int64_t kSession     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kCorrelation = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);

static const uint8_t kGroupId[]    = {'g', 'r', 'p', '2'};
static const uint8_t kMemberId[]   = {'m', 'b', 'r', 'X'};
static const uint8_t kMetaBytes[]  = {0xDE, 0xAD, 0xBE, 0xEF};

// ===========================================================================
// OffsetCommit (templateId 40, BODY_FIXED = 4)
// ===========================================================================

// ---------------------------------------------------------------------------
// OC-1. Round-trip: two entries, one with metadata, one without.
// ---------------------------------------------------------------------------
TEST(OffsetCommitCodec, RoundTripTwoEntries) {
    rpc::OffsetCommit msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.generationId  = 5;
    msg.groupId       = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId      = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));

    rpc::OffsetCommitEntry e1;
    e1.topicId   = 10;
    e1.partition = 0;
    e1.offset    = 1000LL;
    e1.metadata  = std::vector<uint8_t>(kMetaBytes, kMetaBytes + sizeof(kMetaBytes));

    rpc::OffsetCommitEntry e2;
    e2.topicId   = 20;
    e2.partition = 3;
    e2.offset    = 5000LL;
    // e2.metadata empty

    msg.entries = {e1, e2};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    // Decode
    auto decoded = rpc::OffsetCommit::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,       kSession);
    EXPECT_EQ(decoded->correlationId,   kCorrelation);
    EXPECT_EQ(decoded->generationId,    5);
    ASSERT_EQ(decoded->groupId.size(),  sizeof(kGroupId));
    EXPECT_EQ(std::memcmp(decoded->groupId.data(), kGroupId, sizeof(kGroupId)), 0);
    ASSERT_EQ(decoded->memberId.size(), sizeof(kMemberId));
    EXPECT_EQ(std::memcmp(decoded->memberId.data(), kMemberId, sizeof(kMemberId)), 0);

    ASSERT_EQ(decoded->entries.size(), 2u);

    // Entry 0
    EXPECT_EQ(decoded->entries[0].topicId,   10);
    EXPECT_EQ(decoded->entries[0].partition, 0);
    EXPECT_EQ(decoded->entries[0].offset,    1000LL);
    ASSERT_EQ(decoded->entries[0].metadata.size(), sizeof(kMetaBytes));
    EXPECT_EQ(std::memcmp(decoded->entries[0].metadata.data(), kMetaBytes, sizeof(kMetaBytes)), 0);

    // Entry 1
    EXPECT_EQ(decoded->entries[1].topicId,   20);
    EXPECT_EQ(decoded->entries[1].partition, 3);
    EXPECT_EQ(decoded->entries[1].offset,    5000LL);
    EXPECT_EQ(decoded->entries[1].metadata.size(), 0u);
}

// ---------------------------------------------------------------------------
// OC-2. Round-trip: empty entries list.
// ---------------------------------------------------------------------------
TEST(OffsetCommitCodec, EmptyEntries) {
    rpc::OffsetCommit msg;
    msg.generationId = 1;
    msg.groupId      = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId     = std::span<const uint8_t>{};
    msg.entries      = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::OffsetCommit::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->entries.size(), 0u);
}

// ---------------------------------------------------------------------------
// OC-3. Truncation: decode(buf, total-1) → nullopt.
// ---------------------------------------------------------------------------
TEST(OffsetCommitCodec, Truncation) {
    rpc::OffsetCommit msg;
    msg.generationId = 3;
    msg.groupId      = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId     = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));

    rpc::OffsetCommitEntry e;
    e.topicId   = 1;
    e.partition = 0;
    e.offset    = 42LL;
    e.metadata  = {0x01, 0x02};
    msg.entries = {e};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::OffsetCommit::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::OffsetCommit::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// OC-4. Wrong template/schema → nullopt.
// ---------------------------------------------------------------------------
TEST(OffsetCommitCodec, WrongTemplateAndSchema) {
    rpc::OffsetCommit msg;
    msg.generationId = 1;
    msg.groupId      = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId     = std::span<const uint8_t>{};
    msg.entries      = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    // Wrong templateId
    std::vector<uint8_t> buf2 = buf;
    buf2[2] = 41;  // OffsetCommitResponse
    buf2[3] = 0;
    EXPECT_FALSE(rpc::OffsetCommit::decode(buf2.data(), total).has_value());

    // Wrong schemaId
    std::vector<uint8_t> buf3 = buf;
    buf3[4] = 0;
    buf3[5] = 0;
    EXPECT_FALSE(rpc::OffsetCommit::decode(buf3.data(), total).has_value());
}

// ===========================================================================
// OffsetCommitResponse (templateId 41, BODY_FIXED = 0)
// ===========================================================================

TEST(OffsetCommitResponseCodec, RoundTripTwoResults) {
    rpc::OffsetCommitResponse msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.results = {
        {10, 0, 0},
        {20, 3, static_cast<int16_t>(rpc::ErrorCode::UnknownPartition)}
    };

    const std::size_t total = msg.encodedSize();
    // 24 + 0 + 2 + 2*10 = 46
    EXPECT_EQ(total, 46u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::OffsetCommitResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     kSession);
    EXPECT_EQ(decoded->correlationId, kCorrelation);
    ASSERT_EQ(decoded->results.size(), 2u);
    EXPECT_EQ(decoded->results[0].topicId,   10);
    EXPECT_EQ(decoded->results[0].partition, 0);
    EXPECT_EQ(decoded->results[0].errorCode, 0);
    EXPECT_EQ(decoded->results[1].topicId,   20);
    EXPECT_EQ(decoded->results[1].partition, 3);
    EXPECT_EQ(decoded->results[1].errorCode, static_cast<int16_t>(rpc::ErrorCode::UnknownPartition));
}

TEST(OffsetCommitResponseCodec, EmptyResults) {
    rpc::OffsetCommitResponse msg;
    msg.results = {};

    const std::size_t total = msg.encodedSize();
    EXPECT_EQ(total, 26u);  // 24 + 2

    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::OffsetCommitResponse::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->results.size(), 0u);
}

TEST(OffsetCommitResponseCodec, Truncation) {
    rpc::OffsetCommitResponse msg;
    msg.results = { {1, 0, 0}, {2, 1, 0} };

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::OffsetCommitResponse::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::OffsetCommitResponse::decode(buf.data(), total).has_value());
}

TEST(OffsetCommitResponseCodec, WrongTemplate) {
    rpc::OffsetCommitResponse msg;
    msg.results = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[2] = 40;  // OffsetCommit
    buf[3] = 0;
    EXPECT_FALSE(rpc::OffsetCommitResponse::decode(buf.data(), total).has_value());
}

// ===========================================================================
// OffsetFetch (templateId 42, BODY_FIXED = 0)
// ===========================================================================

// ---------------------------------------------------------------------------
// OF-1. Round-trip: two entries (explicit partitions).
// ---------------------------------------------------------------------------
TEST(OffsetFetchCodec, RoundTripTwoEntries) {
    rpc::OffsetFetch msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.groupId       = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.entries       = { {10, 0}, {20, 3} };

    const std::size_t total = msg.encodedSize();
    // 24 + 2 + 4 + 2 + 2*8 = 48
    EXPECT_EQ(total, 24u + 2u + sizeof(kGroupId) + 2u + 2u * 8u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::OffsetFetch::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     kSession);
    EXPECT_EQ(decoded->correlationId, kCorrelation);
    ASSERT_EQ(decoded->groupId.size(), sizeof(kGroupId));
    EXPECT_EQ(std::memcmp(decoded->groupId.data(), kGroupId, sizeof(kGroupId)), 0);
    ASSERT_EQ(decoded->entries.size(), 2u);
    EXPECT_EQ(decoded->entries[0].topicId,   10);
    EXPECT_EQ(decoded->entries[0].partition, 0);
    EXPECT_EQ(decoded->entries[1].topicId,   20);
    EXPECT_EQ(decoded->entries[1].partition, 3);
}

// ---------------------------------------------------------------------------
// OF-2. Round-trip: empty entries = fetch all committed offsets.
// ---------------------------------------------------------------------------
TEST(OffsetFetchCodec, EmptyEntriesFetchAll) {
    rpc::OffsetFetch msg;
    msg.sessionId   = kSession;
    msg.groupId     = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.entries     = {};  // empty = fetch all

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::OffsetFetch::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->entries.size(), 0u);
}

// ---------------------------------------------------------------------------
// OF-3. Truncation: decode(buf, total-1) → nullopt.
// ---------------------------------------------------------------------------
TEST(OffsetFetchCodec, Truncation) {
    rpc::OffsetFetch msg;
    msg.groupId = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.entries = { {5, 2} };

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::OffsetFetch::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::OffsetFetch::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// OF-4. Wrong template → nullopt.
// ---------------------------------------------------------------------------
TEST(OffsetFetchCodec, WrongTemplate) {
    rpc::OffsetFetch msg;
    msg.groupId = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.entries = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[2] = 43;  // OffsetFetchResponse
    buf[3] = 0;

    EXPECT_FALSE(rpc::OffsetFetch::decode(buf.data(), total).has_value());
}

// ===========================================================================
// OffsetFetchResponse (templateId 43, BODY_FIXED = 0)
// ===========================================================================

TEST(OffsetFetchResponseCodec, RoundTripTwoResults) {
    rpc::OffsetFetchResponse msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.results = {
        {10, 0, 99LL,  0},
        {20, 3, 500LL, static_cast<int16_t>(rpc::ErrorCode::UnknownPartition)}
    };

    const std::size_t total = msg.encodedSize();
    // 24 + 2 + 2*18 = 62
    EXPECT_EQ(total, 62u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::OffsetFetchResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     kSession);
    EXPECT_EQ(decoded->correlationId, kCorrelation);
    ASSERT_EQ(decoded->results.size(), 2u);

    EXPECT_EQ(decoded->results[0].topicId,   10);
    EXPECT_EQ(decoded->results[0].partition, 0);
    EXPECT_EQ(decoded->results[0].offset,    99LL);
    EXPECT_EQ(decoded->results[0].errorCode, 0);

    EXPECT_EQ(decoded->results[1].topicId,   20);
    EXPECT_EQ(decoded->results[1].partition, 3);
    EXPECT_EQ(decoded->results[1].offset,    500LL);
    EXPECT_EQ(decoded->results[1].errorCode, static_cast<int16_t>(rpc::ErrorCode::UnknownPartition));
}

TEST(OffsetFetchResponseCodec, Int64OffsetExtrema) {
    rpc::OffsetFetchResponse msg;
    msg.results = {
        {1, 0, INT64_MIN, 0},
        {1, 1, INT64_MAX, 0}
    };

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::OffsetFetchResponse::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->results.size(), 2u);
    EXPECT_EQ(decoded->results[0].offset, INT64_MIN);
    EXPECT_EQ(decoded->results[1].offset, INT64_MAX);
}

TEST(OffsetFetchResponseCodec, EmptyResults) {
    rpc::OffsetFetchResponse msg;
    msg.results = {};

    const std::size_t total = msg.encodedSize();
    EXPECT_EQ(total, 26u);  // 24 + 2

    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::OffsetFetchResponse::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->results.size(), 0u);
}

TEST(OffsetFetchResponseCodec, Truncation) {
    rpc::OffsetFetchResponse msg;
    msg.results = { {1, 0, 42LL, 0} };

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::OffsetFetchResponse::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::OffsetFetchResponse::decode(buf.data(), total).has_value());
}

TEST(OffsetFetchResponseCodec, WrongTemplate) {
    rpc::OffsetFetchResponse msg;
    msg.results = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[2] = 42;  // OffsetFetch
    buf[3] = 0;

    EXPECT_FALSE(rpc::OffsetFetchResponse::decode(buf.data(), total).has_value());
}

TEST(OffsetFetchResponseCodec, WrongSchema) {
    rpc::OffsetFetchResponse msg;
    msg.results = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[4] = 0;
    buf[5] = 0;  // schemaId = 0

    EXPECT_FALSE(rpc::OffsetFetchResponse::decode(buf.data(), total).has_value());
}

// ===========================================================================
// CG-1 Conformance vector for OffsetCommit (with metadata)
// ===========================================================================

// Golden values
static constexpr int64_t kGoldSession     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kGoldCorrelation = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
static const uint8_t kGoldGroupId[]  = {'g', 'r', 'p', '2'};
static const uint8_t kGoldMemberId[] = {'m', 'b', 'r', 'X'};
static const uint8_t kGoldMeta[]     = {0xDE, 0xAD, 0xBE, 0xEF};

// ---------------------------------------------------------------------------
// CV-OFFSET-1: OffsetCommit golden with metadata
//   generationId=5, groupId="grp2", memberId="mbrX"
//   entries: [ (topicId=10, partition=0, offset=1000, meta={DE AD BE EF}) ]
//
// Frame layout:
//   [0..23]  header: blockLen=4, tid=40, schema=120, ver=1, session, correlation
//   [24..27] generationId = 5
//   [28..29] groupId len = 4
//   [30..33] "grp2"
//   [34..35] memberId len = 4
//   [36..39] "mbrX"
//   [40..41] entries count = 1
//   [42..45] topicId = 10
//   [46..49] partition = 0
//   [50..57] offset = 1000
//   [58..59] meta len = 4
//   [60..63] DE AD BE EF
//   Total = 64 bytes
// ---------------------------------------------------------------------------
TEST(OffsetConformance, OffsetCommitWithMetaGolden) {
    rpc::OffsetCommit msg;
    msg.sessionId     = kGoldSession;
    msg.correlationId = kGoldCorrelation;
    msg.generationId  = 5;
    msg.groupId       = std::span<const uint8_t>(kGoldGroupId, sizeof(kGoldGroupId));
    msg.memberId      = std::span<const uint8_t>(kGoldMemberId, sizeof(kGoldMemberId));

    rpc::OffsetCommitEntry e;
    e.topicId   = 10;
    e.partition = 0;
    e.offset    = 1000LL;
    e.metadata  = std::vector<uint8_t>(kGoldMeta, kGoldMeta + sizeof(kGoldMeta));
    msg.entries = {e};

    const std::size_t total = msg.encodedSize();
    ASSERT_EQ(total, 64u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 64u);

    // Header checks
    EXPECT_EQ(leU16(buf.data(),  0), 4u);    // blockLength = BODY_FIXED
    EXPECT_EQ(leU16(buf.data(),  2), 40u);   // templateId = OffsetCommit
    EXPECT_EQ(leU16(buf.data(),  4), 120u);  // schemaId
    EXPECT_EQ(leU16(buf.data(),  6), 1u);    // version
    EXPECT_EQ(leS64(buf.data(),  8), kGoldSession);
    EXPECT_EQ(leS64(buf.data(), 16), kGoldCorrelation);

    // Fixed body
    EXPECT_EQ(leS32(buf.data(), 24), 5);  // generationId

    // groupId = "grp2"
    EXPECT_EQ(leU16(buf.data(), 28), 4u);
    EXPECT_EQ(std::memcmp(buf.data() + 30, kGoldGroupId, 4u), 0);

    // memberId = "mbrX"
    EXPECT_EQ(leU16(buf.data(), 34), 4u);
    EXPECT_EQ(std::memcmp(buf.data() + 36, kGoldMemberId, 4u), 0);

    // entries count = 1
    EXPECT_EQ(leU16(buf.data(), 40), 1u);

    // entry 0: topicId, partition, offset, meta
    EXPECT_EQ(leS32(buf.data(), 42), 10);
    EXPECT_EQ(leS32(buf.data(), 46), 0);
    EXPECT_EQ(leS64(buf.data(), 50), 1000LL);
    EXPECT_EQ(leU16(buf.data(), 58), 4u);
    EXPECT_EQ(std::memcmp(buf.data() + 60, kGoldMeta, 4u), 0);

    // Decode-golden
    auto decoded = rpc::OffsetCommit::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,       kGoldSession);
    EXPECT_EQ(decoded->correlationId,   kGoldCorrelation);
    EXPECT_EQ(decoded->generationId,    5);
    ASSERT_EQ(decoded->groupId.size(),  4u);
    EXPECT_EQ(std::memcmp(decoded->groupId.data(), kGoldGroupId, 4u), 0);
    ASSERT_EQ(decoded->memberId.size(), 4u);
    EXPECT_EQ(std::memcmp(decoded->memberId.data(), kGoldMemberId, 4u), 0);
    ASSERT_EQ(decoded->entries.size(),  1u);
    EXPECT_EQ(decoded->entries[0].topicId,   10);
    EXPECT_EQ(decoded->entries[0].partition, 0);
    EXPECT_EQ(decoded->entries[0].offset,    1000LL);
    ASSERT_EQ(decoded->entries[0].metadata.size(), 4u);
    EXPECT_EQ(std::memcmp(decoded->entries[0].metadata.data(), kGoldMeta, 4u), 0);
}
