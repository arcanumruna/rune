// tests/protocol/ReplicationSchemaV4Test.cpp
// Replication schema 112, version 4 (ISR protocol): the new fields append after the v3 block,
// and the v3 golden frames still decode with the new fields reading as SBE null.
#include <gtest/gtest.h>

#include <rune_protocol_replication_sbe/AlterIsr.h>
#include <rune_protocol_replication_sbe/AlterIsrResponse.h>
#include <rune_protocol_replication_sbe/FetchRequest.h>
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

// Wraps a decoder on a frame: header first, then the body with the header's acting block/version.
template <class Msg>
void wrapFrame(Msg& m, std::vector<char>& frame, std::uint16_t expectBlock, std::uint16_t expectVersion) {
    rsbe::MessageHeader hdr(frame.data(), 0, frame.size(), rsbe::MessageHeader::sbeSchemaVersion());
    ASSERT_EQ(hdr.blockLength(), expectBlock);
    ASSERT_EQ(hdr.version(), expectVersion);
    ASSERT_EQ(hdr.schemaId(), 112);
    m.wrapForDecode(frame.data(), rsbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(), frame.size());
}

constexpr std::int64_t kNull64 = std::numeric_limits<std::int64_t>::min();
constexpr std::int32_t kNull32 = std::numeric_limits<std::int32_t>::min();

}  // namespace

TEST(ReplicationSchemaRegression, AlterIsrV4BlockAppendsFieldsAfterV3Regression) {
    EXPECT_EQ(rsbe::AlterIsr::sbeBlockLength(), 232);
    EXPECT_EQ(rsbe::AlterIsr::partitionEpochEncodingOffset(), 88u);
    EXPECT_EQ(rsbe::AlterIsr::proposalIdEncodingOffset(), 92u);
    EXPECT_EQ(rsbe::AlterIsr::isrStorageEncodingOffset(), 100u);
    EXPECT_EQ(rsbe::AlterIsr::sendSeqEncodingOffset(), 228u);
    EXPECT_EQ(rsbe::AlterIsrResponse::sbeBlockLength(), 46);
    EXPECT_EQ(rsbe::AlterIsrResponse::errorCodeEncodingOffset(), 24u);
    EXPECT_EQ(rsbe::AlterIsrResponse::proposalIdEncodingOffset(), 26u);
    EXPECT_EQ(rsbe::AlterIsrResponse::controllerEpochEncodingOffset(), 34u);
    EXPECT_EQ(rsbe::AlterIsrResponse::sendSeqEncodingOffset(), 42u);
    EXPECT_EQ(rsbe::FetchRequest::sbeBlockLength(), 60);
    EXPECT_EQ(rsbe::FetchRequest::incarnationEncodingOffset(), 36u);
    EXPECT_EQ(rsbe::FetchRequest::storageIdEncodingOffset(), 44u);
    EXPECT_EQ(rsbe::FetchRequest::lastEpochStartOffsetEncodingOffset(), 52u);
}

TEST(ReplicationSchemaRegression, V3VectorsDecodeWithNullNewFieldsRegression) {
    {   // AlterIsr v3
        auto f = loadVector("repl_alter_isr_v3.bin");
        ASSERT_EQ(f.size(), 100u);
        rsbe::AlterIsr m; wrapFrame(m, f, 92, 3);
        EXPECT_EQ(m.topicId(), 1); EXPECT_EQ(m.partition(), 2); EXPECT_EQ(m.leaderId(), 3);
        EXPECT_EQ(m.leaderEpoch(), 4); EXPECT_EQ(m.isrCount(), 2);
        EXPECT_EQ(m.isr(0), 3); EXPECT_EQ(m.isr(1), 5); EXPECT_EQ(m.isr(2), 0);
        EXPECT_EQ(m.partitionEpoch(), 9);
        EXPECT_EQ(m.proposalId(), kNull64);
        EXPECT_EQ(m.sendSeq(), kNull32);
        EXPECT_FALSE(m.isrStorageInActingVersion());
    }
    {   // AlterIsr v4
        auto f = loadVector("repl_alter_isr_v4.bin");
        ASSERT_EQ(f.size(), 240u);
        rsbe::AlterIsr m; wrapFrame(m, f, 232, 4);
        EXPECT_EQ(m.topicId(), 1); EXPECT_EQ(m.partition(), 2); EXPECT_EQ(m.leaderId(), 3);
        EXPECT_EQ(m.leaderEpoch(), 4); EXPECT_EQ(m.isrCount(), 2);
        EXPECT_EQ(m.isr(0), 3); EXPECT_EQ(m.isr(1), 5);
        EXPECT_EQ(m.partitionEpoch(), 9);
        EXPECT_EQ(m.proposalId(), 77);
        EXPECT_EQ(m.isrStorage(0), 11); EXPECT_EQ(m.isrStorage(1), 22); EXPECT_EQ(m.isrStorage(2), 0);
        EXPECT_EQ(m.sendSeq(), 3);
    }
    {   // AlterIsrResponse v3
        auto f = loadVector("repl_alter_isr_response_v3.bin");
        ASSERT_EQ(f.size(), 34u);
        rsbe::AlterIsrResponse m; wrapFrame(m, f, 26, 3);
        EXPECT_EQ(m.topicId(), 1); EXPECT_EQ(m.partition(), 2); EXPECT_EQ(m.leaderId(), 3);
        EXPECT_EQ(m.leaderEpoch(), 4); EXPECT_EQ(m.partitionEpoch(), 9); EXPECT_EQ(m.errorCode(), 2);
        EXPECT_EQ(m.proposalId(), kNull64);
        EXPECT_EQ(m.controllerEpoch(), kNull64);
        EXPECT_EQ(m.sendSeq(), kNull32);
    }
    {   // AlterIsrResponse v4
        auto f = loadVector("repl_alter_isr_response_v4.bin");
        ASSERT_EQ(f.size(), 54u);
        rsbe::AlterIsrResponse m; wrapFrame(m, f, 46, 4);
        EXPECT_EQ(m.topicId(), 1); EXPECT_EQ(m.partition(), 2); EXPECT_EQ(m.leaderId(), 3);
        EXPECT_EQ(m.leaderEpoch(), 4); EXPECT_EQ(m.partitionEpoch(), 9); EXPECT_EQ(m.errorCode(), 2);
        EXPECT_EQ(m.proposalId(), 77); EXPECT_EQ(m.controllerEpoch(), 8); EXPECT_EQ(m.sendSeq(), 3);
    }
    {   // FetchRequest v3
        auto f = loadVector("repl_fetch_request_v3.bin");
        ASSERT_EQ(f.size(), 52u);
        rsbe::FetchRequest m; wrapFrame(m, f, 44, 3);
        EXPECT_EQ(m.leaderEpoch(), 5); EXPECT_EQ(m.partition(), 1); EXPECT_EQ(m.followerId(), 3);
        EXPECT_EQ(m.fetchOffset(), 100); EXPECT_EQ(m.maxBytes(), 4096); EXPECT_EQ(m.topicId(), 7);
        EXPECT_EQ(m.lastFetchedEpoch(), 4); EXPECT_EQ(m.incarnation(), 0x1122);
        EXPECT_EQ(m.storageId(), kNull64);
        EXPECT_EQ(m.lastEpochStartOffset(), kNull64);
    }
    {   // FetchRequest v4
        auto f = loadVector("repl_fetch_request_v4.bin");
        ASSERT_EQ(f.size(), 68u);
        rsbe::FetchRequest m; wrapFrame(m, f, 60, 4);
        EXPECT_EQ(m.leaderEpoch(), 5); EXPECT_EQ(m.partition(), 1); EXPECT_EQ(m.followerId(), 3);
        EXPECT_EQ(m.fetchOffset(), 100); EXPECT_EQ(m.maxBytes(), 4096); EXPECT_EQ(m.topicId(), 7);
        EXPECT_EQ(m.lastFetchedEpoch(), 4); EXPECT_EQ(m.incarnation(), 0x1122);
        EXPECT_EQ(m.storageId(), 0x1234); EXPECT_EQ(m.lastEpochStartOffset(), 90);
    }
}
