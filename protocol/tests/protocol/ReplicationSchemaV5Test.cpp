// tests/protocol/ReplicationSchemaV5Test.cpp
// Replication schema 112, version 5 (#789): FetchResponse appends fromOffset after topicId,
// and the v4 golden frame still decodes with fromOffset reading as SBE null.
#include <gtest/gtest.h>

#include <rune_protocol_replication_sbe/FetchResponse.h>
#include <rune_protocol_replication_sbe/MessageHeader.h>

#include <cstdint>
#include <cstdio>
#include <limits>
#include <string>
#include <vector>

#ifndef RUNE_CONFORMANCE_DIR
#  error "RUNE_CONFORMANCE_DIR must be defined by CMakeLists.txt"
#endif

namespace rsbe = ::rune::protocol::replication::sbe;

namespace {

std::vector<char> loadVector(const char* name) {
    const std::string path = std::string(RUNE_CONFORMANCE_DIR) + "/vectors/" + name;
    std::FILE* f = std::fopen(path.c_str(), "rb");
    EXPECT_NE(f, nullptr) << path;
    std::vector<char> out;
    if (f == nullptr) return out;
    char tmp[512];
    std::size_t n;
    while ((n = std::fread(tmp, 1, sizeof(tmp), f)) > 0) out.insert(out.end(), tmp, tmp + n);
    std::fclose(f);
    return out;
}

void wrapFrame(rsbe::FetchResponse& m, std::vector<char>& frame, std::uint16_t expectBlock, std::uint16_t expectVersion) {
    rsbe::MessageHeader hdr(frame.data(), 0, frame.size(), rsbe::MessageHeader::sbeSchemaVersion());
    ASSERT_EQ(hdr.blockLength(), expectBlock);
    ASSERT_EQ(hdr.version(), expectVersion);
    ASSERT_EQ(hdr.schemaId(), 112);
    ASSERT_EQ(hdr.templateId(), 2);
    m.wrapForDecode(frame.data(), rsbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(), frame.size());
}

void expectCommon(rsbe::FetchResponse& m) {
    EXPECT_EQ(m.leaderEpoch(), 5); EXPECT_EQ(m.partition(), 1); EXPECT_EQ(m.highWatermark(), 200);
    EXPECT_EQ(m.logStartOffset(), 0); EXPECT_EQ(m.errorCode(), 0); EXPECT_EQ(m.topicId(), 7);
    EXPECT_EQ(m.getRecordsAsString(), std::string("\x01\x02\x03", 3));
}

}  // namespace

TEST(ReplicationSchemaRegression, FetchResponseV5BlockAppendsFromOffsetRegression) {
    EXPECT_EQ(rsbe::FetchResponse::sbeSchemaVersion(), 5);
    EXPECT_EQ(rsbe::FetchResponse::sbeBlockLength(), 42);
    EXPECT_EQ(rsbe::FetchResponse::topicIdEncodingOffset(), 30u);
    EXPECT_EQ(rsbe::FetchResponse::fromOffsetEncodingOffset(), 34u);
    EXPECT_EQ(rsbe::FetchResponse::fromOffsetSinceVersion(), 5u);
}

TEST(ReplicationSchemaRegression, FetchResponseV4VectorDecodesWithNullFromOffsetRegression) {
    {   // v4
        auto f = loadVector("repl_fetch_response_v4.bin");
        ASSERT_EQ(f.size(), 49u);
        rsbe::FetchResponse m; wrapFrame(m, f, 34, 4);
        EXPECT_FALSE(m.fromOffsetInActingVersion());
        EXPECT_EQ(m.fromOffset(), std::numeric_limits<std::int64_t>::min());
        expectCommon(m);
    }
    {   // v5
        auto f = loadVector("repl_fetch_response_v5.bin");
        ASSERT_EQ(f.size(), 57u);
        rsbe::FetchResponse m; wrapFrame(m, f, 42, 5);
        EXPECT_EQ(m.fromOffset(), 1047);
        expectCommon(m);
    }
}
