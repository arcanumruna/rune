#pragma once
// ---------------------------------------------------------------------------
// ConsensusMessages.h — Faithful C++ port of Aeron Cluster's SBE consensus
// message protocol (aeron-cluster-codecs.xml, schema id=111, version=17).
//
// Little-endian, fixed-layout SBE encoding/decoding over caller-provided
// buffers. NO internal allocation for fixed-size messages (tryClaim-ready).
// Variable-length CatchupPosition writes into the caller-supplied buffer too.
//
// 8-byte SBE message header:
//   blockLength : uint16   (payload bytes, excluding header)
//   templateId  : uint16
//   schemaId    : uint16   (= 111)
//   version     : uint16   (= 17)
// ---------------------------------------------------------------------------

#include <cstdint>
#include <cstring>
#include <string_view>
#include <cassert>
#include <climits>
#include <bit>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// Schema constants
// ---------------------------------------------------------------------------
inline constexpr uint16_t kSchemaId      = 111;
inline constexpr uint16_t kSchemaVersion = 17;

// ---------------------------------------------------------------------------
// Template IDs
// ---------------------------------------------------------------------------
enum class ConsensusTemplateId : uint16_t {
    CanvassPosition    = 50,
    RequestVote        = 51,
    Vote               = 52,
    NewLeadershipTerm  = 53,
    AppendPosition     = 54,
    CommitPosition     = 55,
    CatchupPosition    = 56,
    StopCatchup        = 57,
    CandidacyState     = 58,   // re-review round 3 CRITICAL-R3 (knowledge exchange)
    StandDown          = 59,   // controlled shutdown: the leader resigns (Rune extension)
    PreVoteRequest     = 60,   // D29 PreVote (Raft §9.6; Rune extension)
    PreVoteResponse    = 61,   // D29
    TermFloorNotice    = 62,   // D29/D24: a member's follow floor is above the leader's term
    ReadLeaseRequest   = 63,   // issue #82 (VLS A.4): a broker asks the controller for a view read lease
    ReadLeaseGrant     = 64,   // issue #82: the controller's quorum-capped grant
    TerminationPosition = 75,
    TerminationAck     = 76,
};

// ---------------------------------------------------------------------------
// BooleanType (SBE enum encodingType=int32)
// ---------------------------------------------------------------------------
enum class BooleanType : int32_t {
    FALSE_VALUE = 0,
    TRUE_VALUE  = 1,
};

// ---------------------------------------------------------------------------
// ConsensusMessageHeader — 8 bytes, little-endian
// ---------------------------------------------------------------------------
struct ConsensusMessageHeader {
    uint16_t blockLength;
    uint16_t templateId;
    uint16_t schemaId;
    uint16_t version;

    static constexpr std::size_t ENCODED_SIZE = 8;

    /// Encode header into buf[offset..offset+8). Returns bytes written (8).
    static std::size_t encode(uint8_t* buf, std::size_t offset,
                              uint16_t blockLen, uint16_t tmplId) noexcept
    {
        auto* p = buf + offset;
        write_u16(p + 0, blockLen);
        write_u16(p + 2, tmplId);
        write_u16(p + 4, kSchemaId);
        write_u16(p + 6, kSchemaVersion);
        return ENCODED_SIZE;
    }

    /// Decode header from buf[offset..offset+8). Returns the header.
    static ConsensusMessageHeader decode(const uint8_t* buf,
                                         std::size_t offset) noexcept
    {
        const auto* p = buf + offset;
        ConsensusMessageHeader h{};
        h.blockLength = read_u16(p + 0);
        h.templateId  = read_u16(p + 2);
        h.schemaId    = read_u16(p + 4);
        h.version     = read_u16(p + 6);
        return h;
    }

private:
    static void write_u16(uint8_t* p, uint16_t v) noexcept {
        std::memcpy(p, &v, 2); // host is LE; identical on LE platforms, correct on BE
        // SBE is little-endian; rewrite if ever targeting big-endian:
        // p[0]=v&0xFF; p[1]=(v>>8)&0xFF;
    }
    static uint16_t read_u16(const uint8_t* p) noexcept {
        uint16_t v; std::memcpy(&v, p, 2); return v;
    }
};

/// Read just the templateId from the SBE header without constructing a struct.
inline uint16_t peekTemplateId(const uint8_t* buf, std::size_t offset = 0) noexcept
{
    uint16_t v;
    std::memcpy(&v, buf + offset + 2, 2);
    return v;
}

// ---------------------------------------------------------------------------
// Internal helpers (little-endian read/write via memcpy — UB-free, optimised)
// ---------------------------------------------------------------------------
namespace detail {

// Enforce little-endian at compile time (schema 111 is little-endian SBE)
static_assert(std::endian::native == std::endian::little,
              "rune::consensus SBE codecs assume a little-endian host (schema 111 is little-endian SBE)");

template<typename T>
inline void write_le(uint8_t* p, T v) noexcept {
    std::memcpy(p, &v, sizeof(T));
}
template<typename T>
inline T read_le(const uint8_t* p) noexcept {
    T v; std::memcpy(&v, p, sizeof(T)); return v;
}

} // namespace detail

// ===========================================================================
// 1. CanvassPosition  (templateId = 50)
//    blockLength = 32
//    Fields (wire order):
//      logLeadershipTermId : int64   @ +0
//      logPosition         : int64   @ +8
//      leadershipTermId    : int64   @ +16
//      followerMemberId    : int32   @ +24
//      protocolVersion     : int32   @ +28  (version_t = int32)
// ===========================================================================
struct CanvassPositionMsg {
    int64_t logLeadershipTermId;
    int64_t logPosition;
    int64_t leadershipTermId;
    int32_t followerMemberId;
    int32_t protocolVersion;

    static constexpr uint16_t TEMPLATE_ID  = 50;
    static constexpr uint16_t BLOCK_LENGTH = 32; // 3×int64 + 2×int32 = 24+8 = 32
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    /// Encode into buf[offset]. Returns total bytes written (header + payload).
    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t logLeadershipTermId_,
                               int64_t logPosition_,
                               int64_t leadershipTermId_,
                               int32_t followerMemberId_,
                               int32_t protocolVersion_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, logLeadershipTermId_);
        detail::write_le<int64_t>(p +  8, logPosition_);
        detail::write_le<int64_t>(p + 16, leadershipTermId_);
        detail::write_le<int32_t>(p + 24, followerMemberId_);
        detail::write_le<int32_t>(p + 28, protocolVersion_);
        return TOTAL_SIZE;
    }

    static CanvassPositionMsg decode(const uint8_t* buf,
                                     std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        CanvassPositionMsg m{};
        m.logLeadershipTermId = detail::read_le<int64_t>(p +  0);
        m.logPosition         = detail::read_le<int64_t>(p +  8);
        m.leadershipTermId    = detail::read_le<int64_t>(p + 16);
        m.followerMemberId    = detail::read_le<int32_t>(p + 24);
        m.protocolVersion     = detail::read_le<int32_t>(p + 28);
        return m;
    }
};

// ===========================================================================
// 2. RequestVote  (templateId = 51)
//    blockLength = 32
//    Fields (wire order):
//      logLeadershipTermId : int64   @ +0
//      logPosition         : int64   @ +8
//      candidateTermId     : int64   @ +16
//      candidateMemberId   : int32   @ +24
//      protocolVersion     : int32   @ +28
// ===========================================================================
struct RequestVoteMsg {
    int64_t logLeadershipTermId;
    int64_t logPosition;
    int64_t candidateTermId;
    int32_t candidateMemberId;
    int32_t protocolVersion;

    static constexpr uint16_t TEMPLATE_ID  = 51;
    static constexpr uint16_t BLOCK_LENGTH = 32;
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t logLeadershipTermId_,
                               int64_t logPosition_,
                               int64_t candidateTermId_,
                               int32_t candidateMemberId_,
                               int32_t protocolVersion_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, logLeadershipTermId_);
        detail::write_le<int64_t>(p +  8, logPosition_);
        detail::write_le<int64_t>(p + 16, candidateTermId_);
        detail::write_le<int32_t>(p + 24, candidateMemberId_);
        detail::write_le<int32_t>(p + 28, protocolVersion_);
        return TOTAL_SIZE;
    }

    static RequestVoteMsg decode(const uint8_t* buf,
                                 std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        RequestVoteMsg m{};
        m.logLeadershipTermId = detail::read_le<int64_t>(p +  0);
        m.logPosition         = detail::read_le<int64_t>(p +  8);
        m.candidateTermId     = detail::read_le<int64_t>(p + 16);
        m.candidateMemberId   = detail::read_le<int32_t>(p + 24);
        m.protocolVersion     = detail::read_le<int32_t>(p + 28);
        return m;
    }
};

// ===========================================================================
// 3. Vote  (templateId = 52)
//    blockLength = 32
//    Fields (wire order):
//      candidateTermId    : int64       @ +0
//      logLeadershipTermId: int64       @ +8
//      logPosition        : int64       @ +16
//      candidateMemberId  : int32       @ +24
//      followerMemberId   : int32       @ +28
//      vote               : BooleanType (int32)  @ +32
//    Total block = 3×8 + 3×4 = 24 + 12 = 36
// ===========================================================================
struct VoteMsg {
    int64_t     candidateTermId;
    int64_t     logLeadershipTermId;
    int64_t     logPosition;
    int32_t     candidateMemberId;
    int32_t     followerMemberId;
    BooleanType vote;

    static constexpr uint16_t TEMPLATE_ID  = 52;
    static constexpr uint16_t BLOCK_LENGTH = 36;
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t     candidateTermId_,
                               int64_t     logLeadershipTermId_,
                               int64_t     logPosition_,
                               int32_t     candidateMemberId_,
                               int32_t     followerMemberId_,
                               BooleanType vote_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, candidateTermId_);
        detail::write_le<int64_t>(p +  8, logLeadershipTermId_);
        detail::write_le<int64_t>(p + 16, logPosition_);
        detail::write_le<int32_t>(p + 24, candidateMemberId_);
        detail::write_le<int32_t>(p + 28, followerMemberId_);
        detail::write_le<int32_t>(p + 32, static_cast<int32_t>(vote_));
        return TOTAL_SIZE;
    }

    static VoteMsg decode(const uint8_t* buf, std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        VoteMsg m{};
        m.candidateTermId     = detail::read_le<int64_t>(p +  0);
        m.logLeadershipTermId = detail::read_le<int64_t>(p +  8);
        m.logPosition         = detail::read_le<int64_t>(p + 16);
        m.candidateMemberId   = detail::read_le<int32_t>(p + 24);
        m.followerMemberId    = detail::read_le<int32_t>(p + 28);
        m.vote = static_cast<BooleanType>(detail::read_le<int32_t>(p + 32));
        return m;
    }
};

// ===========================================================================
// 4. NewLeadershipTerm  (templateId = 53)
//    blockLength = 96 (payload; total on-wire = 104 incl. 8-byte header)
//    (schema field 14 commitPosition is sinceVersion=15, included)
//    Fields (wire order, exact schema field order):
//      logLeadershipTermId    : int64   @ +0
//      nextLeadershipTermId   : int64   @ +8
//      nextTermBaseLogPosition: int64   @ +16
//      nextLogPosition        : int64   @ +24
//      leadershipTermId       : int64   @ +32
//      termBaseLogPosition    : int64   @ +40
//      logPosition            : int64   @ +48
//      leaderRecordingId      : int64   @ +56
//      timestamp              : int64   @ +64   (time_t = int64)
//      leaderMemberId         : int32   @ +72
//      logSessionId           : int32   @ +76
//      appVersion             : int32   @ +80   (version_t = int32)
//      isStartup              : int32   @ +84   (BooleanType = int32)
//      commitPosition         : int64   @ +88   (sinceVersion=15, always encoded)
//    Total: 9×8 + 4×4 + 1×8 = 72 + 16 + 8 = 96
// ===========================================================================
struct NewLeadershipTermMsg {
    int64_t     logLeadershipTermId;
    int64_t     nextLeadershipTermId;
    int64_t     nextTermBaseLogPosition;
    int64_t     nextLogPosition;
    int64_t     leadershipTermId;
    int64_t     termBaseLogPosition;
    int64_t     logPosition;
    int64_t     leaderRecordingId;
    int64_t     timestamp;
    int32_t     leaderMemberId;
    int32_t     logSessionId;
    int32_t     appVersion;
    BooleanType isStartup;
    int64_t     commitPosition;

    static constexpr uint16_t TEMPLATE_ID  = 53;
    // 9 int64s (72) + 4 int32s (16) + 1 int64 commitPosition (8) = 96
    static constexpr uint16_t BLOCK_LENGTH = 96;
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t     logLeadershipTermId_,
                               int64_t     nextLeadershipTermId_,
                               int64_t     nextTermBaseLogPosition_,
                               int64_t     nextLogPosition_,
                               int64_t     leadershipTermId_,
                               int64_t     termBaseLogPosition_,
                               int64_t     logPosition_,
                               int64_t     leaderRecordingId_,
                               int64_t     timestamp_,
                               int32_t     leaderMemberId_,
                               int32_t     logSessionId_,
                               int32_t     appVersion_,
                               BooleanType isStartup_,
                               int64_t     commitPosition_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, logLeadershipTermId_);
        detail::write_le<int64_t>(p +  8, nextLeadershipTermId_);
        detail::write_le<int64_t>(p + 16, nextTermBaseLogPosition_);
        detail::write_le<int64_t>(p + 24, nextLogPosition_);
        detail::write_le<int64_t>(p + 32, leadershipTermId_);
        detail::write_le<int64_t>(p + 40, termBaseLogPosition_);
        detail::write_le<int64_t>(p + 48, logPosition_);
        detail::write_le<int64_t>(p + 56, leaderRecordingId_);
        detail::write_le<int64_t>(p + 64, timestamp_);
        detail::write_le<int32_t>(p + 72, leaderMemberId_);
        detail::write_le<int32_t>(p + 76, logSessionId_);
        detail::write_le<int32_t>(p + 80, appVersion_);
        detail::write_le<int32_t>(p + 84, static_cast<int32_t>(isStartup_));
        detail::write_le<int64_t>(p + 88, commitPosition_);
        return TOTAL_SIZE;
    }

    static NewLeadershipTermMsg decode(const uint8_t* buf,
                                       std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        NewLeadershipTermMsg m{};
        m.logLeadershipTermId    = detail::read_le<int64_t>(p +  0);
        m.nextLeadershipTermId   = detail::read_le<int64_t>(p +  8);
        m.nextTermBaseLogPosition = detail::read_le<int64_t>(p + 16);
        m.nextLogPosition        = detail::read_le<int64_t>(p + 24);
        m.leadershipTermId       = detail::read_le<int64_t>(p + 32);
        m.termBaseLogPosition    = detail::read_le<int64_t>(p + 40);
        m.logPosition            = detail::read_le<int64_t>(p + 48);
        m.leaderRecordingId      = detail::read_le<int64_t>(p + 56);
        m.timestamp              = detail::read_le<int64_t>(p + 64);
        m.leaderMemberId         = detail::read_le<int32_t>(p + 72);
        m.logSessionId           = detail::read_le<int32_t>(p + 76);
        m.appVersion             = detail::read_le<int32_t>(p + 80);
        m.isStartup = static_cast<BooleanType>(detail::read_le<int32_t>(p + 84));
        m.commitPosition         = detail::read_le<int64_t>(p + 88);
        return m;
    }
};

// ===========================================================================
// 5. AppendPosition  (templateId = 54)
//    blockLength = 21  (≤24B payload per schema efficiency comment)
//    Fields (wire order):
//      leadershipTermId : int64   @ +0
//      logPosition      : int64   @ +8
//      followerMemberId : int32   @ +16
//      flags            : uint8   @ +20   (sinceVersion=8, always encoded)
//    Total: 2×8 + 1×4 + 1×1 = 21 bytes — fits in cache line alongside 8B header
// ===========================================================================
struct AppendPositionMsg {
    int64_t leadershipTermId;
    int64_t logPosition;
    int32_t followerMemberId;
    uint8_t flags;

    static constexpr uint16_t TEMPLATE_ID  = 54;
    static constexpr uint16_t BLOCK_LENGTH = 21; // ≤24B payload
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static_assert(BLOCK_LENGTH <= 24,
        "AppendPosition payload must be ≤24 bytes (schema efficiency constraint)");

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t leadershipTermId_,
                               int64_t logPosition_,
                               int32_t followerMemberId_,
                               uint8_t flags_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, leadershipTermId_);
        detail::write_le<int64_t>(p +  8, logPosition_);
        detail::write_le<int32_t>(p + 16, followerMemberId_);
        p[20] = flags_;
        return TOTAL_SIZE;
    }

    static AppendPositionMsg decode(const uint8_t* buf,
                                    std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        AppendPositionMsg m{};
        m.leadershipTermId = detail::read_le<int64_t>(p +  0);
        m.logPosition      = detail::read_le<int64_t>(p +  8);
        m.followerMemberId = detail::read_le<int32_t>(p + 16);
        m.flags            = p[20];
        return m;
    }
};

// ===========================================================================
// 6. CommitPosition  (templateId = 55)
//    blockLength = 20  (≤24B payload per schema efficiency comment)
//    Fields (wire order):
//      leadershipTermId : int64   @ +0
//      logPosition      : int64   @ +8
//      leaderMemberId   : int32   @ +16
//    Total: 2×8 + 1×4 = 20 bytes
// ===========================================================================
struct CommitPositionMsg {
    int64_t leadershipTermId;
    int64_t logPosition;
    int32_t leaderMemberId;

    static constexpr uint16_t TEMPLATE_ID  = 55;
    static constexpr uint16_t BLOCK_LENGTH = 20; // ≤24B payload
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static_assert(BLOCK_LENGTH <= 24,
        "CommitPosition payload must be ≤24 bytes (schema efficiency constraint)");

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t leadershipTermId_,
                               int64_t logPosition_,
                               int32_t leaderMemberId_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, leadershipTermId_);
        detail::write_le<int64_t>(p +  8, logPosition_);
        detail::write_le<int32_t>(p + 16, leaderMemberId_);
        return TOTAL_SIZE;
    }

    static CommitPositionMsg decode(const uint8_t* buf,
                                    std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        CommitPositionMsg m{};
        m.leadershipTermId = detail::read_le<int64_t>(p + 0);
        m.logPosition      = detail::read_le<int64_t>(p + 8);
        m.leaderMemberId   = detail::read_le<int32_t>(p + 16);
        return m;
    }
};

// ===========================================================================
// 7. CatchupPosition  (templateId = 56)  — variable-length
//    Fixed fields block = 20 bytes:
//      leadershipTermId : int64   @ +0
//      logPosition      : int64   @ +8
//      followerMemberId : int32   @ +16
//    Followed by varAsciiEncoding (catchupEndpoint):
//      length    : uint32   @ +20
//      varData   : uint8[]  @ +24  (length bytes, US-ASCII)
//    Total = 8 + 20 + 4 + strlen(endpoint)
// ===========================================================================
struct CatchupPositionMsg {
    int64_t          leadershipTermId;
    int64_t          logPosition;
    int32_t          followerMemberId;
    std::string_view catchupEndpoint;  // points into a decode buffer; zero-copy

    static constexpr uint16_t TEMPLATE_ID        = 56;
    static constexpr uint16_t BLOCK_LENGTH_FIXED  = 20; // fixed fields only
    // varAsciiEncoding length prefix is uint32 (4 bytes)
    static constexpr std::size_t VAR_HEADER_SIZE  = 4;

    /// Returns total encoded bytes written into buf[offset].
    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t     leadershipTermId_,
                               int64_t     logPosition_,
                               int32_t     followerMemberId_,
                               std::string_view catchupEndpoint_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH_FIXED, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, leadershipTermId_);
        detail::write_le<int64_t>(p +  8, logPosition_);
        detail::write_le<int32_t>(p + 16, followerMemberId_);
        pos += BLOCK_LENGTH_FIXED;
        // varAsciiEncoding: uint32 length then raw bytes
        auto len = static_cast<uint32_t>(catchupEndpoint_.size());
        detail::write_le<uint32_t>(buf + pos, len);
        pos += VAR_HEADER_SIZE;
        std::memcpy(buf + pos, catchupEndpoint_.data(), len);
        pos += len;
        return pos - offset;
    }

    /// Decode — catchupEndpoint is a view into buf (zero-copy).
    static CatchupPositionMsg decode(const uint8_t* buf,
                                     std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        CatchupPositionMsg m{};
        m.leadershipTermId = detail::read_le<int64_t>(p +  0);
        m.logPosition      = detail::read_le<int64_t>(p +  8);
        m.followerMemberId = detail::read_le<int32_t>(p + 16);
        const uint8_t* vp = p + BLOCK_LENGTH_FIXED;
        uint32_t len = detail::read_le<uint32_t>(vp);
        m.catchupEndpoint  = std::string_view{
            reinterpret_cast<const char*>(vp + VAR_HEADER_SIZE), len};
        return m;
    }
};

// ===========================================================================
// 8. StopCatchup  (templateId = 57)
//    blockLength = 12
//    Fields (wire order):
//      leadershipTermId : int64   @ +0
//      followerMemberId : int32   @ +8
// ===========================================================================
struct StopCatchupMsg {
    int64_t leadershipTermId;
    int32_t followerMemberId;

    static constexpr uint16_t TEMPLATE_ID  = 57;
    static constexpr uint16_t BLOCK_LENGTH = 12;
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t leadershipTermId_,
                               int32_t followerMemberId_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p + 0, leadershipTermId_);
        detail::write_le<int32_t>(p + 8, followerMemberId_);
        return TOTAL_SIZE;
    }

    static StopCatchupMsg decode(const uint8_t* buf,
                                 std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        StopCatchupMsg m{};
        m.leadershipTermId = detail::read_le<int64_t>(p + 0);
        m.followerMemberId = detail::read_le<int32_t>(p + 8);
        return m;
    }
};

// ===========================================================================
// 9. TerminationPosition  (templateId = 75)
//    blockLength = 16
//    Fields (wire order):
//      leadershipTermId : int64   @ +0
//      logPosition      : int64   @ +8
// ===========================================================================
struct TerminationPositionMsg {
    int64_t leadershipTermId;
    int64_t logPosition;

    static constexpr uint16_t TEMPLATE_ID  = 75;
    static constexpr uint16_t BLOCK_LENGTH = 16;
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t leadershipTermId_,
                               int64_t logPosition_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p + 0, leadershipTermId_);
        detail::write_le<int64_t>(p + 8, logPosition_);
        return TOTAL_SIZE;
    }

    static TerminationPositionMsg decode(const uint8_t* buf,
                                         std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        TerminationPositionMsg m{};
        m.leadershipTermId = detail::read_le<int64_t>(p + 0);
        m.logPosition      = detail::read_le<int64_t>(p + 8);
        return m;
    }
};

// ===========================================================================
// 10. TerminationAck  (templateId = 76)
//    blockLength = 20
//    Fields (wire order):
//      leadershipTermId : int64   @ +0
//      logPosition      : int64   @ +8
//      memberId         : int32   @ +16
// ===========================================================================
struct TerminationAckMsg {
    int64_t leadershipTermId;
    int64_t logPosition;
    int32_t memberId;

    static constexpr uint16_t TEMPLATE_ID  = 76;
    static constexpr uint16_t BLOCK_LENGTH = 20;
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               int64_t leadershipTermId_,
                               int64_t logPosition_,
                               int32_t memberId_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, leadershipTermId_);
        detail::write_le<int64_t>(p +  8, logPosition_);
        detail::write_le<int32_t>(p + 16, memberId_);
        return TOTAL_SIZE;
    }

    static TerminationAckMsg decode(const uint8_t* buf,
                                    std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        TerminationAckMsg m{};
        m.leadershipTermId = detail::read_le<int64_t>(p +  0);
        m.logPosition      = detail::read_le<int64_t>(p +  8);
        m.memberId         = detail::read_le<int32_t>(p + 16);
        return m;
    }
};


// ===========================================================================
// CandidacyState  (templateId = 58) — re-review 2026-10-04 round 3
// CRITICAL-R3: the sender's newest durable __cluster_metadata ISR state,
// exchanged while canvassing / voting (see CandidacyState.h).
//    blockLength = 88
//      leaderEpoch : int64      @ +0
//      version     : int64      @ +8
//      fromMember  : int32      @ +16
//      count       : int32      @ +20
//      members     : int32[16]  @ +24
// ===========================================================================
struct CandidacyStateMsg {
    int64_t leaderEpoch;
    int64_t version;
    int32_t fromMember;
    int32_t count;
    int32_t members[16];

    static constexpr uint16_t TEMPLATE_ID  = 58;
    static constexpr uint16_t BLOCK_LENGTH = 88;
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset, int64_t leaderEpoch_,
                              int64_t version_, int32_t fromMember_, int32_t count_,
                              const int32_t* members_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, leaderEpoch_);
        detail::write_le<int64_t>(p +  8, version_);
        detail::write_le<int32_t>(p + 16, fromMember_);
        detail::write_le<int32_t>(p + 20, count_);
        for (int i = 0; i < 16; ++i)
            detail::write_le<int32_t>(p + 24 + 4 * i, i < count_ ? members_[i] : -1);
        return TOTAL_SIZE;
    }

    static CandidacyStateMsg decode(const uint8_t* buf, std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        CandidacyStateMsg m{};
        m.leaderEpoch = detail::read_le<int64_t>(p +  0);
        m.version     = detail::read_le<int64_t>(p +  8);
        m.fromMember  = detail::read_le<int32_t>(p + 16);
        m.count       = detail::read_le<int32_t>(p + 20);
        for (int i = 0; i < 16; ++i) m.members[i] = detail::read_le<int32_t>(p + 24 + 4 * i);
        return m;
    }
};

// ===========================================================================
// PreVoteRequest  (templateId = 60) — D29 PreVote (Raft thesis §9.6), a Rune
// extension.  A would-be candidate asks "would you vote for me at `term`?"
// BEFORE raising its durable candidate term; nobody's durable state changes.
//    blockLength = 32
//      term                : int64  @ +0
//      logLeadershipTermId : int64  @ +8
//      logPosition         : int64  @ +16
//      candidateId         : int32  @ +24
//      (pad)               : int32  @ +28
// ===========================================================================
struct PreVoteRequestMsg {
    int64_t term;
    int64_t logLeadershipTermId;
    int64_t logPosition;
    int32_t candidateId;

    static constexpr uint16_t TEMPLATE_ID  = 60;
    static constexpr uint16_t BLOCK_LENGTH = 32;
    static constexpr std::size_t TOTAL_SIZE = ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset, int64_t term_, int64_t llt_, int64_t pos_,
                              int32_t candidateId_) noexcept {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, term_);
        detail::write_le<int64_t>(p +  8, llt_);
        detail::write_le<int64_t>(p + 16, pos_);
        detail::write_le<int32_t>(p + 24, candidateId_);
        detail::write_le<int32_t>(p + 28, 0);
        return TOTAL_SIZE;
    }
    static PreVoteRequestMsg decode(const uint8_t* buf, std::size_t offset) noexcept {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        PreVoteRequestMsg m{};
        m.term                = detail::read_le<int64_t>(p +  0);
        m.logLeadershipTermId = detail::read_le<int64_t>(p +  8);
        m.logPosition         = detail::read_le<int64_t>(p + 16);
        m.candidateId         = detail::read_le<int32_t>(p + 24);
        return m;
    }
};

// ===========================================================================
// PreVoteResponse  (templateId = 61) — D29: the voter's non-binding answer,
// with the voter's own durable term (etcd: a rejected pre-vote carries it) so a
// candidate behind a live member's term catches up — Aeron's term catch-up ran
// through the stale node's real RequestVote, which PreVote no longer sends.
//    blockLength = 24
//      term        : int64  @ +0
//      voterTermId : int64  @ +8   (max(candidate term, follow floor) of the voter)
//      voterId     : int32  @ +16
//      granted     : int32  @ +20  (0 / 1)
// ===========================================================================
struct PreVoteResponseMsg {
    int64_t term;
    int64_t voterTermId;
    int32_t voterId;
    int32_t granted;

    static constexpr uint16_t TEMPLATE_ID  = 61;
    static constexpr uint16_t BLOCK_LENGTH = 24;
    static constexpr std::size_t TOTAL_SIZE = ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset, int64_t term_, int64_t voterTermId_,
                              int32_t voterId_, bool granted_) noexcept {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p + 0, term_);
        detail::write_le<int64_t>(p + 8, voterTermId_);
        detail::write_le<int32_t>(p + 16, voterId_);
        detail::write_le<int32_t>(p + 20, granted_ ? 1 : 0);
        return TOTAL_SIZE;
    }
    static PreVoteResponseMsg decode(const uint8_t* buf, std::size_t offset) noexcept {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        PreVoteResponseMsg m{};
        m.term        = detail::read_le<int64_t>(p + 0);
        m.voterTermId = detail::read_le<int64_t>(p + 8);
        m.voterId     = detail::read_le<int32_t>(p + 16);
        m.granted     = detail::read_le<int32_t>(p + 20);
        return m;
    }
};

// ===========================================================================
// TermFloorNotice  (templateId = 62) — D29/D24, a Rune extension (etcd's
// "message from a lower-term leader" answer, narrowed to the durable follow
// floor; safe with PreVote — a cut-off candidate can no longer ratchet a
// bridge member's floor).  Sent by a member that refused the leader's
// NewLeadershipTerm for `refusedTermId` because it granted, led, followed or
// adopted a newer term (`floorTermId`); that leader steps down.
//    blockLength = 24
//      floorTermId   : int64  @ +0
//      refusedTermId : int64  @ +8
//      fromMember    : int32  @ +16
//      (pad)         : int32  @ +20
// ===========================================================================
struct TermFloorNoticeMsg {
    int64_t floorTermId;
    int64_t refusedTermId;
    int32_t fromMember;

    static constexpr uint16_t TEMPLATE_ID  = 62;
    static constexpr uint16_t BLOCK_LENGTH = 24;
    static constexpr std::size_t TOTAL_SIZE = ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset, int64_t floorTermId_, int64_t refusedTermId_,
                              int32_t fromMember_) noexcept {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, floorTermId_);
        detail::write_le<int64_t>(p +  8, refusedTermId_);
        detail::write_le<int32_t>(p + 16, fromMember_);
        detail::write_le<int32_t>(p + 20, 0);
        return TOTAL_SIZE;
    }
    static TermFloorNoticeMsg decode(const uint8_t* buf, std::size_t offset) noexcept {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        TermFloorNoticeMsg m{};
        m.floorTermId   = detail::read_le<int64_t>(p +  0);
        m.refusedTermId = detail::read_le<int64_t>(p +  8);
        m.fromMember    = detail::read_le<int32_t>(p + 16);
        return m;
    }
};

// ===========================================================================
// StandDown  (templateId = 59) — controlled shutdown (2026-10-05).  The leader
// of `leadershipTermId` resigns before it exits: a follower of it starts an
// election at once, skipping it (Aeron gracefulClosedLeaderId).  A Rune
// extension (no Aeron equivalent: Aeron closes the whole cluster); an older
// node ignores the unknown template and times the leader out as before.
//    blockLength = 12
//      leadershipTermId : int64   @ +0
//      leaderMemberId   : int32   @ +8
// ===========================================================================
struct StandDownMsg {
    int64_t leadershipTermId;
    int32_t leaderMemberId;

    static constexpr uint16_t TEMPLATE_ID  = 59;
    static constexpr uint16_t BLOCK_LENGTH = 12;
    static constexpr std::size_t TOTAL_SIZE =
        ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset, int64_t leadershipTermId_,
                              int32_t leaderMemberId_) noexcept
    {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p + 0, leadershipTermId_);
        detail::write_le<int32_t>(p + 8, leaderMemberId_);
        return TOTAL_SIZE;
    }

    static StandDownMsg decode(const uint8_t* buf, std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        StandDownMsg m{};
        m.leadershipTermId = detail::read_le<int64_t>(p + 0);
        m.leaderMemberId   = detail::read_le<int32_t>(p + 8);
        return m;
    }
};


// ===========================================================================
// ReadLeaseRequest  (templateId = 63) — issue #82 (VLS Part A, A.4; the spec's
// 62 is TermFloorNotice).  A Rune extension, sent by a CLOSED follower with
// every AppendPosition (FollowerLiveness cadence): it asks the controller for a
// view read lease and echoes the highest grant it heard (the controller's QL).
// An older controller ignores the unknown template: no grant, so view reads
// answer NotLeader until the controller is upgraded (documented, like PreVote).
//    blockLength = 32
//      term          : int64  @ +0   (the leadership term the sender follows)
//      reqSeq        : int64  @ +8
//      heardGrantSeq : int64  @ +16  (0 = none heard from this leader)
//      memberId      : int32  @ +24  (the sender)
//      (pad)         : int32  @ +28
// ===========================================================================
struct ReadLeaseRequestMsg {
    int64_t term;
    int64_t reqSeq;
    int64_t heardGrantSeq;
    int32_t memberId;

    static constexpr uint16_t TEMPLATE_ID  = 63;
    static constexpr uint16_t BLOCK_LENGTH = 32;
    static constexpr std::size_t TOTAL_SIZE = ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset, int64_t term_, int64_t reqSeq_, int64_t heardGrantSeq_,
                              int32_t memberId_) noexcept {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, term_);
        detail::write_le<int64_t>(p +  8, reqSeq_);
        detail::write_le<int64_t>(p + 16, heardGrantSeq_);
        detail::write_le<int32_t>(p + 24, memberId_);
        detail::write_le<int32_t>(p + 28, 0);
        return TOTAL_SIZE;
    }
    static ReadLeaseRequestMsg decode(const uint8_t* buf, std::size_t offset) noexcept {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        ReadLeaseRequestMsg m{};
        m.term          = detail::read_le<int64_t>(p +  0);
        m.reqSeq        = detail::read_le<int64_t>(p +  8);
        m.heardGrantSeq = detail::read_le<int64_t>(p + 16);
        m.memberId      = detail::read_le<int32_t>(p + 24);
        return m;
    }
};

// ===========================================================================
// ReadLeaseGrant  (templateId = 64) — issue #82: the controller's answer to one
// ReadLeaseRequest.  validForNs (controller lease clock) is capped by the quorum
// lease QL; the broker's lease ends at its request's send time + validForNs /
// (1+rho)^2.  commitPos = the controller's metadata log end at grant time: the
// broker hands the lease to a worker only once every role op derived from the
// metadata below it is on that worker's op ring.  A grant is also a leader
// heartbeat for the recipient (a C4 "heard" source).
//    blockLength = 48
//      term           : int64  @ +0
//      reqSeq         : int64  @ +8   (the request answered)
//      grantSeq       : int64  @ +16  (echoed back by the next requests)
//      commitPos      : int64  @ +24
//      validForNs     : int64  @ +32  (0 = heard, no lease)
//      memberId       : int32  @ +40  (the recipient)
//      leaderMemberId : int32  @ +44  (the sender: the controller)
// ===========================================================================
struct ReadLeaseGrantMsg {
    int64_t term;
    int64_t reqSeq;
    int64_t grantSeq;
    int64_t commitPos;
    int64_t validForNs;
    int32_t memberId;
    int32_t leaderMemberId;

    static constexpr uint16_t TEMPLATE_ID  = 64;
    static constexpr uint16_t BLOCK_LENGTH = 48;
    static constexpr std::size_t TOTAL_SIZE = ConsensusMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset, int64_t term_, int64_t reqSeq_, int64_t grantSeq_,
                              int64_t commitPos_, int64_t validForNs_, int32_t memberId_, int32_t leaderMemberId_) noexcept {
        std::size_t pos = offset;
        pos += ConsensusMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, term_);
        detail::write_le<int64_t>(p +  8, reqSeq_);
        detail::write_le<int64_t>(p + 16, grantSeq_);
        detail::write_le<int64_t>(p + 24, commitPos_);
        detail::write_le<int64_t>(p + 32, validForNs_);
        detail::write_le<int32_t>(p + 40, memberId_);
        detail::write_le<int32_t>(p + 44, leaderMemberId_);
        return TOTAL_SIZE;
    }
    static ReadLeaseGrantMsg decode(const uint8_t* buf, std::size_t offset) noexcept {
        const uint8_t* p = buf + offset + ConsensusMessageHeader::ENCODED_SIZE;
        ReadLeaseGrantMsg m{};
        m.term           = detail::read_le<int64_t>(p +  0);
        m.reqSeq         = detail::read_le<int64_t>(p +  8);
        m.grantSeq       = detail::read_le<int64_t>(p + 16);
        m.commitPos      = detail::read_le<int64_t>(p + 24);
        m.validForNs     = detail::read_le<int64_t>(p + 32);
        m.memberId       = detail::read_le<int32_t>(p + 40);
        m.leaderMemberId = detail::read_le<int32_t>(p + 44);
        return m;
    }
};

} // namespace rune::consensus
