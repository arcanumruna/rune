// ConsensusMessagesTest.cpp — TDD tests for the faithful SBE port of
// Aeron Cluster consensus messages (schema id=111, little-endian).
//
// Strategy:
//  - Use distinct sentinel values per field to catch offset bugs.
//  - Verify round-trip encode→decode returns every field byte-exact.
//  - Verify header: correct templateId, schemaId=111, blockLength.
//  - Verify encoded length == TOTAL_SIZE (and ≤24B payload for 54/55).
//  - peekTemplateId returns correct id for every message.
//  - CatchupPosition variable-length: empty, non-empty, special chars.
//  - Little-endian byte layout spot-check for CanvassPosition.
//  - Boundary values: INT64_MIN/MAX, negative member ids, vote bool variants.

#include <gtest/gtest.h>
#include <rune/consensus/ConsensusMessages.h>
#include <array>
#include <cstring>
#include <climits>
#include <string>

using namespace rune::consensus;

static constexpr std::size_t BUF_SIZE = 512;

// ---------------------------------------------------------------------------
// Header tests
// ---------------------------------------------------------------------------

TEST(ConsensusMessageHeader, EncodeDecodeSmokeTest)
{
    std::array<uint8_t, 8> buf{};
    ConsensusMessageHeader::encode(buf.data(), 0, 42, 50);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.blockLength, 42);
    EXPECT_EQ(h.templateId,  50);
    EXPECT_EQ(h.schemaId,    kSchemaId);      // 111
    EXPECT_EQ(h.version,     kSchemaVersion); // 17
}

TEST(ConsensusMessageHeader, SchemaId111)
{
    std::array<uint8_t, 8> buf{};
    ConsensusMessageHeader::encode(buf.data(), 0, 0, 99);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.schemaId, uint16_t{111});
}

TEST(ConsensusMessageHeader, EncodedSizeIs8)
{
    EXPECT_EQ(ConsensusMessageHeader::ENCODED_SIZE, std::size_t{8});
}

// ---------------------------------------------------------------------------
// peekTemplateId tests
// ---------------------------------------------------------------------------

TEST(PeekTemplateId, CanvassPosition)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CanvassPositionMsg::encode(buf.data(), 0, 1, 2, 3, 4, 5);
    EXPECT_EQ(peekTemplateId(buf.data(), 0),
              static_cast<uint16_t>(ConsensusTemplateId::CanvassPosition));
}

TEST(PeekTemplateId, RequestVote)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    RequestVoteMsg::encode(buf.data(), 0, 1, 2, 3, 4, 5);
    EXPECT_EQ(peekTemplateId(buf.data(), 0), uint16_t{51});
}

TEST(PeekTemplateId, Vote)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    VoteMsg::encode(buf.data(), 0, 1, 2, 3, 4, 5, BooleanType::TRUE_VALUE);
    EXPECT_EQ(peekTemplateId(buf.data(), 0), uint16_t{52});
}

TEST(PeekTemplateId, NewLeadershipTerm)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    NewLeadershipTermMsg::encode(buf.data(), 0,
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, BooleanType::FALSE_VALUE, 14);
    EXPECT_EQ(peekTemplateId(buf.data(), 0), uint16_t{53});
}

TEST(PeekTemplateId, AppendPosition)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    AppendPositionMsg::encode(buf.data(), 0, 1, 2, 3, 0x01);
    EXPECT_EQ(peekTemplateId(buf.data(), 0), uint16_t{54});
}

TEST(PeekTemplateId, CommitPosition)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CommitPositionMsg::encode(buf.data(), 0, 1, 2, 3);
    EXPECT_EQ(peekTemplateId(buf.data(), 0), uint16_t{55});
}

TEST(PeekTemplateId, CatchupPosition)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CatchupPositionMsg::encode(buf.data(), 0, 1, 2, 3, "ep:9090");
    EXPECT_EQ(peekTemplateId(buf.data(), 0), uint16_t{56});
}

TEST(PeekTemplateId, StopCatchup)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    StopCatchupMsg::encode(buf.data(), 0, 1, 2);
    EXPECT_EQ(peekTemplateId(buf.data(), 0), uint16_t{57});
}

TEST(PeekTemplateId, TerminationPosition)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    TerminationPositionMsg::encode(buf.data(), 0, 1, 2);
    EXPECT_EQ(peekTemplateId(buf.data(), 0), uint16_t{75});
}

TEST(PeekTemplateId, TerminationAck)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    TerminationAckMsg::encode(buf.data(), 0, 1, 2, 3);
    EXPECT_EQ(peekTemplateId(buf.data(), 0), uint16_t{76});
}

// ---------------------------------------------------------------------------
// CanvassPosition (templateId=50) tests
// ---------------------------------------------------------------------------

TEST(CanvassPosition, RoundTrip)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    const int64_t logLTId  = 0x1122334455667788LL;
    const int64_t logPos   = 0x99AABBCCDDEEFF00LL;
    const int64_t ltId     = 0x0102030405060708LL;
    const int32_t follower = 0x11223344;
    const int32_t proto    = 0x55667788;

    auto written = CanvassPositionMsg::encode(buf.data(), 0,
        logLTId, logPos, ltId, follower, proto);
    EXPECT_EQ(written, CanvassPositionMsg::TOTAL_SIZE);

    auto m = CanvassPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.logLeadershipTermId, logLTId);
    EXPECT_EQ(m.logPosition,         logPos);
    EXPECT_EQ(m.leadershipTermId,    ltId);
    EXPECT_EQ(m.followerMemberId,    follower);
    EXPECT_EQ(m.protocolVersion,     proto);
}

TEST(CanvassPosition, HeaderFields)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CanvassPositionMsg::encode(buf.data(), 0, 0, 0, 0, 0, 0);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{50});
    EXPECT_EQ(h.schemaId,    uint16_t{111});
    EXPECT_EQ(h.blockLength, CanvassPositionMsg::BLOCK_LENGTH);
}

TEST(CanvassPosition, EncodedLength)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto written = CanvassPositionMsg::encode(buf.data(), 0, 1, 2, 3, 4, 5);
    EXPECT_EQ(written, CanvassPositionMsg::TOTAL_SIZE);
    // Header 8 + payload 32 = 40
    EXPECT_EQ(written, std::size_t{40});
}

// Spot-check little-endian byte layout for CanvassPosition
// logLeadershipTermId=0x0807060504030201 → bytes: 01 02 03 04 05 06 07 08 at buf[8..15]
TEST(CanvassPosition, LittleEndianByteLayout)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CanvassPositionMsg::encode(buf.data(), 0,
        /*logLeadershipTermId=*/INT64_C(0x0807060504030201),
        /*logPosition=*/        INT64_C(0x1716151413121110),
        /*leadershipTermId=*/   INT64_C(0x2726252423222120),
        /*followerMemberId=*/   INT32_C(0x31302928),
        /*protocolVersion=*/    INT32_C(0x35343332));

    // header is 8 bytes; payload starts at index 8
    EXPECT_EQ(buf[ 8], 0x01); // logLeadershipTermId LE byte 0
    EXPECT_EQ(buf[ 9], 0x02);
    EXPECT_EQ(buf[10], 0x03);
    EXPECT_EQ(buf[11], 0x04);
    EXPECT_EQ(buf[12], 0x05);
    EXPECT_EQ(buf[13], 0x06);
    EXPECT_EQ(buf[14], 0x07);
    EXPECT_EQ(buf[15], 0x08); // logLeadershipTermId LE byte 7

    EXPECT_EQ(buf[16], 0x10); // logPosition LE byte 0
    EXPECT_EQ(buf[23], 0x17); // logPosition LE byte 7

    EXPECT_EQ(buf[32], 0x28); // followerMemberId LE byte 0
    EXPECT_EQ(buf[33], 0x29);
    EXPECT_EQ(buf[34], 0x30);
    EXPECT_EQ(buf[35], 0x31);

    EXPECT_EQ(buf[36], 0x32); // protocolVersion LE byte 0
    EXPECT_EQ(buf[39], 0x35); // protocolVersion LE byte 3
}

TEST(CanvassPosition, BoundaryValues)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CanvassPositionMsg::encode(buf.data(), 0,
        INT64_MIN, INT64_MAX, INT64_C(-1), INT32_MIN, INT32_MAX);
    auto m = CanvassPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.logLeadershipTermId, INT64_MIN);
    EXPECT_EQ(m.logPosition,         INT64_MAX);
    EXPECT_EQ(m.leadershipTermId,    INT64_C(-1));
    EXPECT_EQ(m.followerMemberId,    INT32_MIN);
    EXPECT_EQ(m.protocolVersion,     INT32_MAX);
}

// ---------------------------------------------------------------------------
// RequestVote (templateId=51) tests
// ---------------------------------------------------------------------------

TEST(RequestVote, RoundTrip)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    const int64_t logLTId = 0xAABBCCDD11223344LL;
    const int64_t logPos  = 0x1234567890ABCDEFLL;
    const int64_t candTId = 0xDEADBEEFCAFEBABELL;
    const int32_t candId  = 0x00000042;
    const int32_t proto   = 0x00000011;

    RequestVoteMsg::encode(buf.data(), 0, logLTId, logPos, candTId, candId, proto);
    auto m = RequestVoteMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.logLeadershipTermId, logLTId);
    EXPECT_EQ(m.logPosition,         logPos);
    EXPECT_EQ(m.candidateTermId,     candTId);
    EXPECT_EQ(m.candidateMemberId,   candId);
    EXPECT_EQ(m.protocolVersion,     proto);
}

TEST(RequestVote, HeaderTemplateId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    RequestVoteMsg::encode(buf.data(), 0, 0, 0, 0, 0, 0);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId, uint16_t{51});
    EXPECT_EQ(h.schemaId,   uint16_t{111});
}

TEST(RequestVote, EncodedLength)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto n = RequestVoteMsg::encode(buf.data(), 0, 1, 2, 3, 4, 5);
    EXPECT_EQ(n, RequestVoteMsg::TOTAL_SIZE);
    EXPECT_EQ(n, std::size_t{40}); // 8 header + 32 payload
}

TEST(RequestVote, NegativeMemberId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    RequestVoteMsg::encode(buf.data(), 0, 0, 0, 0, -7, 0);
    auto m = RequestVoteMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.candidateMemberId, -7);
}

// ---------------------------------------------------------------------------
// Vote (templateId=52) tests
// ---------------------------------------------------------------------------

TEST(Vote, RoundTripVoteTrue)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    VoteMsg::encode(buf.data(), 0,
        /*candidateTermId=*/    INT64_C(0x1111222233334444),
        /*logLeadershipTermId=*/INT64_C(0x5555666677778888),
        /*logPosition=*/        INT64_C(0x9999AAAABBBBCCCC),
        /*candidateMemberId=*/  7,
        /*followerMemberId=*/   3,
        /*vote=*/               BooleanType::TRUE_VALUE);
    auto m = VoteMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.candidateTermId,     INT64_C(0x1111222233334444));
    EXPECT_EQ(m.logLeadershipTermId, INT64_C(0x5555666677778888));
    EXPECT_EQ(m.logPosition,         INT64_C(0x9999AAAABBBBCCCC));
    EXPECT_EQ(m.candidateMemberId,   7);
    EXPECT_EQ(m.followerMemberId,    3);
    EXPECT_EQ(m.vote,                BooleanType::TRUE_VALUE);
}

TEST(Vote, RoundTripVoteFalse)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    VoteMsg::encode(buf.data(), 0, 1, 2, 3, 4, 5, BooleanType::FALSE_VALUE);
    auto m = VoteMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.vote, BooleanType::FALSE_VALUE);
}

TEST(Vote, HeaderTemplateId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    VoteMsg::encode(buf.data(), 0, 0, 0, 0, 0, 0, BooleanType::FALSE_VALUE);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId, uint16_t{52});
    EXPECT_EQ(h.schemaId,   uint16_t{111});
    EXPECT_EQ(h.blockLength, VoteMsg::BLOCK_LENGTH);
}

TEST(Vote, EncodedLength)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto n = VoteMsg::encode(buf.data(), 0, 1, 2, 3, 4, 5, BooleanType::TRUE_VALUE);
    EXPECT_EQ(n, VoteMsg::TOTAL_SIZE);
    EXPECT_EQ(n, std::size_t{44}); // 8 header + 36 payload
}

TEST(Vote, VoteFieldIsInt32InWire)
{
    // BooleanType is int32; verify the wire value for TRUE_VALUE = 1 in LE
    std::array<uint8_t, BUF_SIZE> buf{};
    VoteMsg::encode(buf.data(), 0, 0, 0, 0, 0, 0, BooleanType::TRUE_VALUE);
    // vote is at header(8) + 3×8 + 2×4 = 8 + 24 + 8 = 40 offset
    EXPECT_EQ(buf[8 + 32], 0x01); // LE byte 0 of int32(1)
    EXPECT_EQ(buf[8 + 33], 0x00);
    EXPECT_EQ(buf[8 + 34], 0x00);
    EXPECT_EQ(buf[8 + 35], 0x00);
}

// ---------------------------------------------------------------------------
// NewLeadershipTerm (templateId=53) tests
// ---------------------------------------------------------------------------

TEST(NewLeadershipTerm, RoundTrip)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    NewLeadershipTermMsg::encode(buf.data(), 0,
        /*logLeadershipTermId=*/    INT64_C(0x0102030405060708),
        /*nextLeadershipTermId=*/   INT64_C(0x1112131415161718),
        /*nextTermBaseLogPosition=*/INT64_C(0x2122232425262728),
        /*nextLogPosition=*/        INT64_C(0x3132333435363738),
        /*leadershipTermId=*/       INT64_C(0x4142434445464748),
        /*termBaseLogPosition=*/    INT64_C(0x5152535455565758),
        /*logPosition=*/            INT64_C(0x6162636465666768),
        /*leaderRecordingId=*/      INT64_C(0x7172737475767778),
        /*timestamp=*/              INT64_C(0x8182838485868788),
        /*leaderMemberId=*/         INT32_C(0x91929394),
        /*logSessionId=*/           INT32_C(0xA1A2A3A4),
        /*appVersion=*/             INT32_C(0xB1B2B3B4),
        /*isStartup=*/              BooleanType::TRUE_VALUE,
        /*commitPosition=*/         INT64_C(0xC1C2C3C4C5C6C7C8));

    auto m = NewLeadershipTermMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.logLeadershipTermId,    INT64_C(0x0102030405060708));
    EXPECT_EQ(m.nextLeadershipTermId,   INT64_C(0x1112131415161718));
    EXPECT_EQ(m.nextTermBaseLogPosition,INT64_C(0x2122232425262728));
    EXPECT_EQ(m.nextLogPosition,        INT64_C(0x3132333435363738));
    EXPECT_EQ(m.leadershipTermId,       INT64_C(0x4142434445464748));
    EXPECT_EQ(m.termBaseLogPosition,    INT64_C(0x5152535455565758));
    EXPECT_EQ(m.logPosition,            INT64_C(0x6162636465666768));
    EXPECT_EQ(m.leaderRecordingId,      INT64_C(0x7172737475767778));
    EXPECT_EQ(m.timestamp,              INT64_C(0x8182838485868788));
    EXPECT_EQ(m.leaderMemberId,         INT32_C(0x91929394));
    EXPECT_EQ(m.logSessionId,           INT32_C(0xA1A2A3A4));
    EXPECT_EQ(m.appVersion,             INT32_C(0xB1B2B3B4));
    EXPECT_EQ(m.isStartup,              BooleanType::TRUE_VALUE);
    EXPECT_EQ(m.commitPosition,         INT64_C(0xC1C2C3C4C5C6C7C8));
}

TEST(NewLeadershipTerm, HeaderTemplateId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    NewLeadershipTermMsg::encode(buf.data(), 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, BooleanType::FALSE_VALUE, 0);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{53});
    EXPECT_EQ(h.schemaId,    uint16_t{111});
    EXPECT_EQ(h.blockLength, NewLeadershipTermMsg::BLOCK_LENGTH);
}

TEST(NewLeadershipTerm, EncodedLength)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto n = NewLeadershipTermMsg::encode(buf.data(), 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, BooleanType::FALSE_VALUE, 0);
    EXPECT_EQ(n, NewLeadershipTermMsg::TOTAL_SIZE);
    // 8 header + 96 payload = 104
    EXPECT_EQ(n, std::size_t{104});
}

TEST(NewLeadershipTerm, IsStartupFalse)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    NewLeadershipTermMsg::encode(buf.data(), 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, BooleanType::FALSE_VALUE, 0);
    auto m = NewLeadershipTermMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.isStartup, BooleanType::FALSE_VALUE);
}

TEST(NewLeadershipTerm, CommitPositionField)
{
    // commitPosition is the last field (sinceVersion=15, always included in v17)
    std::array<uint8_t, BUF_SIZE> buf{};
    NewLeadershipTermMsg::encode(buf.data(), 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, BooleanType::FALSE_VALUE,
        INT64_C(0xFEDCBA9876543210));
    auto m = NewLeadershipTermMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.commitPosition, INT64_C(0xFEDCBA9876543210));
}

TEST(NewLeadershipTerm, BoundaryValues)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    NewLeadershipTermMsg::encode(buf.data(), 0,
        INT64_MIN, INT64_MAX, INT64_MIN, INT64_MAX,
        INT64_MIN, INT64_MAX, INT64_MIN, INT64_MAX,
        INT64_MIN, INT32_MIN, INT32_MAX, INT32_C(0),
        BooleanType::TRUE_VALUE, INT64_MAX);
    auto m = NewLeadershipTermMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.logLeadershipTermId, INT64_MIN);
    EXPECT_EQ(m.nextLeadershipTermId, INT64_MAX);
    EXPECT_EQ(m.leaderMemberId, INT32_MIN);
    EXPECT_EQ(m.logSessionId, INT32_MAX);
    EXPECT_EQ(m.commitPosition, INT64_MAX);
}

// ---------------------------------------------------------------------------
// AppendPosition (templateId=54) tests — ≤24B payload
// ---------------------------------------------------------------------------

TEST(AppendPosition, RoundTrip)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    AppendPositionMsg::encode(buf.data(), 0,
        /*leadershipTermId=*/INT64_C(0xAABBCCDDEEFF0011),
        /*logPosition=*/     INT64_C(0x2233445566778899),
        /*followerMemberId=*/INT32_C(0x00AB00CD),
        /*flags=*/           uint8_t{0x01});
    auto m = AppendPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.leadershipTermId, INT64_C(0xAABBCCDDEEFF0011));
    EXPECT_EQ(m.logPosition,      INT64_C(0x2233445566778899));
    EXPECT_EQ(m.followerMemberId, INT32_C(0x00AB00CD));
    EXPECT_EQ(m.flags,            uint8_t{0x01});
}

TEST(AppendPosition, HeaderTemplateId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    AppendPositionMsg::encode(buf.data(), 0, 0, 0, 0, 0);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{54});
    EXPECT_EQ(h.schemaId,    uint16_t{111});
    EXPECT_EQ(h.blockLength, AppendPositionMsg::BLOCK_LENGTH);
}

TEST(AppendPosition, PayloadAt24ByteConstraint)
{
    EXPECT_LE(AppendPositionMsg::BLOCK_LENGTH, uint16_t{24});
    // Total encoded ≤ 32 bytes (8 header + 24 payload)
    EXPECT_LE(AppendPositionMsg::TOTAL_SIZE, std::size_t{32});
}

TEST(AppendPosition, EncodedLength)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto n = AppendPositionMsg::encode(buf.data(), 0, 1, 2, 3, 4);
    EXPECT_EQ(n, AppendPositionMsg::TOTAL_SIZE);
    EXPECT_EQ(n, std::size_t{29}); // 8 header + 21 payload
}

TEST(AppendPosition, FlagsNone)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    AppendPositionMsg::encode(buf.data(), 0, 0, 0, 0, uint8_t{0});
    auto m = AppendPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.flags, uint8_t{0});
}

TEST(AppendPosition, FlagsCatchup)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    AppendPositionMsg::encode(buf.data(), 0, 0, 0, 0, uint8_t{1});
    auto m = AppendPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.flags, uint8_t{1});
}

// ---------------------------------------------------------------------------
// CommitPosition (templateId=55) tests — ≤24B payload
// ---------------------------------------------------------------------------

TEST(CommitPosition, RoundTrip)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CommitPositionMsg::encode(buf.data(), 0,
        /*leadershipTermId=*/INT64_C(0x1234567890ABCDEF),
        /*logPosition=*/     INT64_C(0xFEDCBA0987654321),
        /*leaderMemberId=*/  INT32_C(0x42424242));
    auto m = CommitPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.leadershipTermId, INT64_C(0x1234567890ABCDEF));
    EXPECT_EQ(m.logPosition,      INT64_C(0xFEDCBA0987654321));
    EXPECT_EQ(m.leaderMemberId,   INT32_C(0x42424242));
}

TEST(CommitPosition, HeaderTemplateId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CommitPositionMsg::encode(buf.data(), 0, 0, 0, 0);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{55});
    EXPECT_EQ(h.schemaId,    uint16_t{111});
    EXPECT_EQ(h.blockLength, CommitPositionMsg::BLOCK_LENGTH);
}

TEST(CommitPosition, PayloadAt24ByteConstraint)
{
    EXPECT_LE(CommitPositionMsg::BLOCK_LENGTH, uint16_t{24});
}

TEST(CommitPosition, EncodedLength)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto n = CommitPositionMsg::encode(buf.data(), 0, 1, 2, 3);
    EXPECT_EQ(n, CommitPositionMsg::TOTAL_SIZE);
    EXPECT_EQ(n, std::size_t{28}); // 8 header + 20 payload
}

// ---------------------------------------------------------------------------
// CatchupPosition (templateId=56) tests — variable-length
// ---------------------------------------------------------------------------

TEST(CatchupPosition, RoundTripNonEmpty)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CatchupPositionMsg::encode(buf.data(), 0,
        /*leadershipTermId=*/INT64_C(0x1122334455667788),
        /*logPosition=*/     INT64_C(0x99AABBCCDDEEFF00),
        /*followerMemberId=*/INT32_C(0x00112233),
        "localhost:40123");
    auto m = CatchupPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.leadershipTermId, INT64_C(0x1122334455667788));
    EXPECT_EQ(m.logPosition,      INT64_C(0x99AABBCCDDEEFF00));
    EXPECT_EQ(m.followerMemberId, INT32_C(0x00112233));
    EXPECT_EQ(m.catchupEndpoint,  std::string_view{"localhost:40123"});
}

TEST(CatchupPosition, RoundTripEmpty)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CatchupPositionMsg::encode(buf.data(), 0, 5, 10, 1, "");
    auto m = CatchupPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.leadershipTermId, int64_t{5});
    EXPECT_EQ(m.logPosition,      int64_t{10});
    EXPECT_EQ(m.followerMemberId, int32_t{1});
    EXPECT_EQ(m.catchupEndpoint.size(), std::size_t{0});
    EXPECT_EQ(m.catchupEndpoint, std::string_view{""});
}

TEST(CatchupPosition, SpecialCharsEndpoint)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    std::string ep = "aeron:udp?endpoint=192.168.1.1:9090|mtu=1408";
    CatchupPositionMsg::encode(buf.data(), 0, 99, 200, 3, ep);
    auto m = CatchupPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(std::string{m.catchupEndpoint}, ep);
}

TEST(CatchupPosition, HeaderTemplateId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CatchupPositionMsg::encode(buf.data(), 0, 0, 0, 0, "test");
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId, uint16_t{56});
    EXPECT_EQ(h.schemaId,   uint16_t{111});
}

TEST(CatchupPosition, EncodedLengthMatchesContentSize)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    std::string ep = "node1:9000";
    auto n = CatchupPositionMsg::encode(buf.data(), 0, 1, 2, 3, ep);
    // 8 header + 20 fixed + 4 var-len prefix + strlen(ep) = 32 + 10 = 42
    EXPECT_EQ(n, std::size_t{8 + 20 + 4 + ep.size()});
}

TEST(CatchupPosition, EncodedLengthEmpty)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto n = CatchupPositionMsg::encode(buf.data(), 0, 1, 2, 3, "");
    EXPECT_EQ(n, std::size_t{8 + 20 + 4}); // 32
}

// ---------------------------------------------------------------------------
// StopCatchup (templateId=57) tests
// ---------------------------------------------------------------------------

TEST(StopCatchup, RoundTrip)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    StopCatchupMsg::encode(buf.data(), 0,
        /*leadershipTermId=*/INT64_C(0x1111222233334444),
        /*followerMemberId=*/INT32_C(0x55667788));
    auto m = StopCatchupMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.leadershipTermId, INT64_C(0x1111222233334444));
    EXPECT_EQ(m.followerMemberId, INT32_C(0x55667788));
}

TEST(StopCatchup, HeaderTemplateId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    StopCatchupMsg::encode(buf.data(), 0, 0, 0);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{57});
    EXPECT_EQ(h.schemaId,    uint16_t{111});
    EXPECT_EQ(h.blockLength, StopCatchupMsg::BLOCK_LENGTH);
}

TEST(StopCatchup, EncodedLength)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto n = StopCatchupMsg::encode(buf.data(), 0, 1, 2);
    EXPECT_EQ(n, StopCatchupMsg::TOTAL_SIZE);
    EXPECT_EQ(n, std::size_t{20}); // 8 header + 12 payload
}

TEST(StopCatchup, NegativeFollowerMemberId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    StopCatchupMsg::encode(buf.data(), 0, 0, -1);
    auto m = StopCatchupMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.followerMemberId, -1);
}

// ---------------------------------------------------------------------------
// TerminationPosition (templateId=75) tests
// ---------------------------------------------------------------------------

TEST(TerminationPosition, RoundTrip)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    TerminationPositionMsg::encode(buf.data(), 0,
        /*leadershipTermId=*/INT64_C(0xCAFEBABEDEADBEEF),
        /*logPosition=*/     INT64_C(0x0123456789ABCDEF));
    auto m = TerminationPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.leadershipTermId, INT64_C(0xCAFEBABEDEADBEEF));
    EXPECT_EQ(m.logPosition,      INT64_C(0x0123456789ABCDEF));
}

TEST(TerminationPosition, HeaderTemplateId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    TerminationPositionMsg::encode(buf.data(), 0, 0, 0);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{75});
    EXPECT_EQ(h.schemaId,    uint16_t{111});
    EXPECT_EQ(h.blockLength, TerminationPositionMsg::BLOCK_LENGTH);
}

TEST(TerminationPosition, EncodedLength)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto n = TerminationPositionMsg::encode(buf.data(), 0, 1, 2);
    EXPECT_EQ(n, TerminationPositionMsg::TOTAL_SIZE);
    EXPECT_EQ(n, std::size_t{24}); // 8 header + 16 payload
}

TEST(TerminationPosition, BoundaryValues)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    TerminationPositionMsg::encode(buf.data(), 0, INT64_MIN, INT64_MAX);
    auto m = TerminationPositionMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.leadershipTermId, INT64_MIN);
    EXPECT_EQ(m.logPosition,      INT64_MAX);
}

// ---------------------------------------------------------------------------
// TerminationAck (templateId=76) tests
// ---------------------------------------------------------------------------

TEST(TerminationAck, RoundTrip)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    TerminationAckMsg::encode(buf.data(), 0,
        /*leadershipTermId=*/INT64_C(0xFEDCBA9876543210),
        /*logPosition=*/     INT64_C(0x0102030405060708),
        /*memberId=*/        INT32_C(0x00000099));
    auto m = TerminationAckMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.leadershipTermId, INT64_C(0xFEDCBA9876543210));
    EXPECT_EQ(m.logPosition,      INT64_C(0x0102030405060708));
    EXPECT_EQ(m.memberId,         INT32_C(0x00000099));
}

TEST(TerminationAck, HeaderTemplateId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    TerminationAckMsg::encode(buf.data(), 0, 0, 0, 0);
    auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId,  uint16_t{76});
    EXPECT_EQ(h.schemaId,    uint16_t{111});
    EXPECT_EQ(h.blockLength, TerminationAckMsg::BLOCK_LENGTH);
}

TEST(TerminationAck, EncodedLength)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    auto n = TerminationAckMsg::encode(buf.data(), 0, 1, 2, 3);
    EXPECT_EQ(n, TerminationAckMsg::TOTAL_SIZE);
    EXPECT_EQ(n, std::size_t{28}); // 8 header + 20 payload
}

TEST(TerminationAck, NegativeMemberId)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    TerminationAckMsg::encode(buf.data(), 0, 0, 0, -42);
    auto m = TerminationAckMsg::decode(buf.data(), 0);
    EXPECT_EQ(m.memberId, -42);
}

// ---------------------------------------------------------------------------
// Non-zero offset encode/decode
// ---------------------------------------------------------------------------

TEST(NonZeroOffset, CanvassPositionAtOffset16)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CanvassPositionMsg::encode(buf.data(), 16, 10, 20, 30, 4, 5);
    // First 16 bytes must be untouched (zeroed)
    for (int i = 0; i < 16; ++i) EXPECT_EQ(buf[i], 0) << "byte " << i;
    auto m = CanvassPositionMsg::decode(buf.data(), 16);
    EXPECT_EQ(m.logLeadershipTermId, 10);
    EXPECT_EQ(m.logPosition,         20);
    EXPECT_EQ(m.leadershipTermId,    30);
}

TEST(NonZeroOffset, CommitPositionAtOffset32)
{
    std::array<uint8_t, BUF_SIZE> buf{};
    CommitPositionMsg::encode(buf.data(), 32, 100, 200, 7);
    auto m = CommitPositionMsg::decode(buf.data(), 32);
    EXPECT_EQ(m.leadershipTermId, 100);
    EXPECT_EQ(m.logPosition,      200);
    EXPECT_EQ(m.leaderMemberId,   7);
}

// ---------------------------------------------------------------------------
// ConsensusTemplateId enum values
// ---------------------------------------------------------------------------

TEST(ConsensusTemplateId, EnumValues)
{
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::CanvassPosition),    uint16_t{50});
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::RequestVote),        uint16_t{51});
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::Vote),               uint16_t{52});
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::NewLeadershipTerm),  uint16_t{53});
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::AppendPosition),     uint16_t{54});
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::CommitPosition),     uint16_t{55});
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::CatchupPosition),    uint16_t{56});
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::StopCatchup),        uint16_t{57});
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::TerminationPosition),uint16_t{75});
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::TerminationAck),     uint16_t{76});
}

// Re-review 2026-10-04 (fix round 3) CRITICAL-R3: CandidacyState (template 58)
// carries the sender's newest metadata-ISR state; it round-trips through the
// codec and the adapter hands it to a handler that models the exchange.
#include <rune/consensus/ConsensusAdapter.h>
#include <rune/consensus/SbeConsensusPublisher.h>
namespace {
struct CandidacyHandler {
    int got = 0; NodeId from = -1; rune::consensus::CandidacyState st{};
    void onCanvassPosition(int64_t, Term, int64_t, Term, NodeId, int32_t) {}
    void onRequestVote(int64_t, Term, int64_t, Term, NodeId, int32_t) {}
    void onVote(int64_t, Term, Term, int64_t, NodeId, NodeId, bool) {}
    void onNewLeadershipTerm(int64_t, Term, Term, int64_t, int64_t, Term, int64_t, int64_t, int64_t,
                             int64_t, NodeId, int32_t, int32_t, bool, int64_t) {}
    void onAppendPosition(int64_t, Term, int64_t, NodeId, uint8_t) {}
    void onCommitPosition(int64_t, Term, int64_t, NodeId) {}
    void onCandidacyState(int64_t, NodeId f, const rune::consensus::CandidacyState& s) { ++got; from = f; st = s; }
};
} // namespace

TEST(ConsensusMessagesRegression, CandidacyStateRoundTripsThroughCodecAndAdapter) {
    std::array<uint8_t, rune::consensus::kEncodeBufCapacity> buf{};
    const int32_t members[3] = {1, 3, 5};
    const std::size_t len = CandidacyStateMsg::encode(buf.data(), 0, /*leaderEpoch=*/7, /*version=*/4,
                                                      /*fromMember=*/2, /*count=*/3, members);
    ASSERT_EQ(len, CandidacyStateMsg::TOTAL_SIZE);
    ASSERT_LE(len, rune::consensus::kEncodeBufCapacity);
    CandidacyHandler h;
    BasicConsensusAdapter<CandidacyHandler> adapter{h};
    adapter.onFragment(0, buf.data(), len);
    ASSERT_EQ(h.got, 1);
    EXPECT_EQ(h.from, 2);
    EXPECT_EQ(h.st.leaderEpoch, 7);
    EXPECT_EQ(h.st.version, 4);
    ASSERT_EQ(h.st.count, 3);
    EXPECT_EQ(h.st.members[0], 1);
    EXPECT_EQ(h.st.members[2], 5);
    adapter.onFragment(0, buf.data(), len - 1);   // truncated: ignored
    EXPECT_EQ(h.got, 1);
    EXPECT_EQ(adapter.ignoredFrameCount(), 1);
}
