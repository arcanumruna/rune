// ConsensusPublisherAdapterTest.cpp — Publisher↔Adapter loopback integration tests.
//
// Strategy:
//  - LoopbackSink captures (toMember, bytes) from SbeConsensusPublisher.
//  - RecordingHandler captures each onXxx call's fields from ConsensusAdapter.
//  - For each of the 6 messages: publish → capture → adapt → assert exact fields.
//  - Additional: dispatch correctness, buffer reuse, toMember addressing.
//
// All 6 messages use distinct sentinel values per field to catch offset bugs.

#include <gtest/gtest.h>
#include <rune/consensus/SbeConsensusPublisher.h>
#include <rune/consensus/ConsensusAdapter.h>
#include <rune/consensus/MessageSink.h>
#include <rune/consensus/ConsensusMessageHandler.h>

#include <vector>
#include <cstdint>
#include <cstring>
#include <optional>

using namespace rune::consensus;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

struct Capture {
    NodeId              toMember{};
    std::vector<uint8_t> bytes{};
};

/// In-memory sink: records the last send() call.
class LoopbackSink final : public MessageSink {
public:
    void send(NodeId toMember, const uint8_t* buf, std::size_t len) override {
        last_.toMember = toMember;
        last_.bytes.assign(buf, buf + len);
        callCount_++;
    }

    const Capture& last()    const { return last_; }
    int            calls()   const { return callCount_; }

private:
    Capture last_{};
    int     callCount_{0};
};

/// Tracks which handler method was called and with what fields.
struct HandlerRecord {
    enum class Which {
        None, Canvass, RequestVote, Vote, NewLeadershipTerm, AppendPosition, CommitPosition
    } which{Which::None};

    // CanvassPosition fields
    int64_t nowNs_canvass{};
    Term    logLeadershipTermId_canvass{};
    int64_t logPosition_canvass{};
    Term    leadershipTermId_canvass{};
    NodeId  followerMemberId_canvass{};
    int32_t protocolVersion_canvass{};

    // RequestVote fields
    int64_t nowNs_rv{};
    Term    logLeadershipTermId_rv{};
    int64_t logPosition_rv{};
    Term    candidateTermId_rv{};
    NodeId  candidateMemberId_rv{};
    int32_t protocolVersion_rv{};

    // Vote fields
    int64_t nowNs_vote{};
    Term    candidateTermId_vote{};
    Term    logLeadershipTermId_vote{};
    int64_t logPosition_vote{};
    NodeId  candidateMemberId_vote{};
    NodeId  followerMemberId_vote{};
    bool    vote_vote{};

    // NewLeadershipTerm fields
    int64_t nowNs_nlt{};
    Term    logLeadershipTermId_nlt{};
    Term    nextLeadershipTermId_nlt{};
    int64_t nextTermBaseLogPosition_nlt{};
    int64_t nextLogPosition_nlt{};
    Term    leadershipTermId_nlt{};
    int64_t termBaseLogPosition_nlt{};
    int64_t logPosition_nlt{};
    int64_t leaderRecordingId_nlt{};
    int64_t timestamp_nlt{};
    NodeId  leaderMemberId_nlt{};
    int32_t logSessionId_nlt{};
    int32_t appVersion_nlt{};
    bool    isStartup_nlt{};
    int64_t commitPosition_nlt{};

    // AppendPosition fields
    int64_t nowNs_ap{};
    Term    leadershipTermId_ap{};
    int64_t logPosition_ap{};
    NodeId  followerMemberId_ap{};
    uint8_t flags_ap{};

    // CommitPosition fields
    int64_t nowNs_cp{};
    Term    leadershipTermId_cp{};
    int64_t logPosition_cp{};
    NodeId  leaderMemberId_cp{};
};

class RecordingHandler final : public ConsensusMessageHandler {
public:
    void onCanvassPosition(int64_t nowNs, Term logLeadershipTermId, int64_t logPosition,
                           Term leadershipTermId, NodeId followerMemberId,
                           int32_t protocolVersion) override {
        r_.which = HandlerRecord::Which::Canvass;
        r_.nowNs_canvass = nowNs;
        r_.logLeadershipTermId_canvass = logLeadershipTermId;
        r_.logPosition_canvass = logPosition;
        r_.leadershipTermId_canvass = leadershipTermId;
        r_.followerMemberId_canvass = followerMemberId;
        r_.protocolVersion_canvass = protocolVersion;
        callCount_++;
    }

    void onRequestVote(int64_t nowNs, Term logLeadershipTermId, int64_t logPosition,
                       Term candidateTermId, NodeId candidateMemberId,
                       int32_t protocolVersion) override {
        r_.which = HandlerRecord::Which::RequestVote;
        r_.nowNs_rv = nowNs;
        r_.logLeadershipTermId_rv = logLeadershipTermId;
        r_.logPosition_rv = logPosition;
        r_.candidateTermId_rv = candidateTermId;
        r_.candidateMemberId_rv = candidateMemberId;
        r_.protocolVersion_rv = protocolVersion;
        callCount_++;
    }

    void onVote(int64_t nowNs, Term candidateTermId, Term logLeadershipTermId,
                int64_t logPosition, NodeId candidateMemberId, NodeId followerMemberId,
                bool vote) override {
        r_.which = HandlerRecord::Which::Vote;
        r_.nowNs_vote = nowNs;
        r_.candidateTermId_vote = candidateTermId;
        r_.logLeadershipTermId_vote = logLeadershipTermId;
        r_.logPosition_vote = logPosition;
        r_.candidateMemberId_vote = candidateMemberId;
        r_.followerMemberId_vote = followerMemberId;
        r_.vote_vote = vote;
        callCount_++;
    }

    void onNewLeadershipTerm(int64_t nowNs, Term logLeadershipTermId,
                             Term nextLeadershipTermId, int64_t nextTermBaseLogPosition,
                             int64_t nextLogPosition, Term leadershipTermId,
                             int64_t termBaseLogPosition, int64_t logPosition,
                             int64_t leaderRecordingId, int64_t timestamp,
                             NodeId leaderMemberId, int32_t logSessionId,
                             int32_t appVersion, bool isStartup,
                             int64_t commitPosition) override {
        r_.which = HandlerRecord::Which::NewLeadershipTerm;
        r_.nowNs_nlt = nowNs;
        r_.logLeadershipTermId_nlt = logLeadershipTermId;
        r_.nextLeadershipTermId_nlt = nextLeadershipTermId;
        r_.nextTermBaseLogPosition_nlt = nextTermBaseLogPosition;
        r_.nextLogPosition_nlt = nextLogPosition;
        r_.leadershipTermId_nlt = leadershipTermId;
        r_.termBaseLogPosition_nlt = termBaseLogPosition;
        r_.logPosition_nlt = logPosition;
        r_.leaderRecordingId_nlt = leaderRecordingId;
        r_.timestamp_nlt = timestamp;
        r_.leaderMemberId_nlt = leaderMemberId;
        r_.logSessionId_nlt = logSessionId;
        r_.appVersion_nlt = appVersion;
        r_.isStartup_nlt = isStartup;
        r_.commitPosition_nlt = commitPosition;
        callCount_++;
    }

    void onAppendPosition(int64_t nowNs, Term leadershipTermId, int64_t logPosition,
                          NodeId followerMemberId, uint8_t flags) override {
        r_.which = HandlerRecord::Which::AppendPosition;
        r_.nowNs_ap = nowNs;
        r_.leadershipTermId_ap = leadershipTermId;
        r_.logPosition_ap = logPosition;
        r_.followerMemberId_ap = followerMemberId;
        r_.flags_ap = flags;
        callCount_++;
    }

    void onCommitPosition(int64_t nowNs, Term leadershipTermId, int64_t logPosition,
                          NodeId leaderMemberId) override {
        r_.which = HandlerRecord::Which::CommitPosition;
        r_.nowNs_cp = nowNs;
        r_.leadershipTermId_cp = leadershipTermId;
        r_.logPosition_cp = logPosition;
        r_.leaderMemberId_cp = leaderMemberId;
        callCount_++;
    }

    const HandlerRecord& record() const { return r_; }
    int calls() const { return callCount_; }
    void reset() { r_ = {}; callCount_ = 0; }

private:
    HandlerRecord r_{};
    int           callCount_{0};
};

// Fixture providing a publisher + adapter backed by the same recording infrastructure.
class PublisherAdapterFixture : public ::testing::Test {
protected:
    LoopbackSink      sink_{};
    RecordingHandler  handler_{};
    SbeConsensusPublisher publisher_{sink_};
    ConsensusAdapter      adapter_{handler_};

    static constexpr int64_t kNow = INT64_C(0xABCDEF0123456789);

    void loopback() {
        const auto& cap = sink_.last();
        adapter_.onFragment(kNow, cap.bytes.data(), cap.bytes.size());
    }
};

// ---------------------------------------------------------------------------
// 1. CanvassPosition round-trip
// ---------------------------------------------------------------------------

TEST_F(PublisherAdapterFixture, CanvassPosition_RoundTrip)
{
    publisher_.canvassPosition(
        /*toMember=*/   7,
        /*logLeadershipTermId=*/ INT64_C(0x1111111111111111),
        /*logPosition=*/         INT64_C(0x2222222222222222),
        /*leadershipTermId=*/    INT64_C(0x3333333333333333),
        /*followerMemberId=*/    42);

    loopback();

    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::Canvass);
    EXPECT_EQ(handler_.record().nowNs_canvass,               kNow);
    EXPECT_EQ(handler_.record().logLeadershipTermId_canvass, INT64_C(0x1111111111111111));
    EXPECT_EQ(handler_.record().logPosition_canvass,         INT64_C(0x2222222222222222));
    EXPECT_EQ(handler_.record().leadershipTermId_canvass,    INT64_C(0x3333333333333333));
    EXPECT_EQ(handler_.record().followerMemberId_canvass,    42);
}

TEST_F(PublisherAdapterFixture, CanvassPosition_ToMemberPreserved)
{
    publisher_.canvassPosition(99, 0, 0, 0, 0);
    EXPECT_EQ(sink_.last().toMember, 99);
}

TEST_F(PublisherAdapterFixture, CanvassPosition_DispatchesOnlyCanvass)
{
    publisher_.canvassPosition(1, 0, 0, 0, 0);
    loopback();
    EXPECT_EQ(handler_.calls(), 1);
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::Canvass);
}

// ---------------------------------------------------------------------------
// 2. RequestVote round-trip
// ---------------------------------------------------------------------------

TEST_F(PublisherAdapterFixture, RequestVote_RoundTrip)
{
    publisher_.requestVote(
        /*toMember=*/           3,
        /*logLeadershipTermId=*/INT64_C(0xAAAAAAAAAAAAAAAA),
        /*logPosition=*/        INT64_C(0xBBBBBBBBBBBBBBBB),
        /*candidateTermId=*/    INT64_C(0xCCCCCCCCCCCCCCCC),
        /*candidateMemberId=*/  5);

    loopback();

    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::RequestVote);
    EXPECT_EQ(handler_.record().nowNs_rv,               kNow);
    EXPECT_EQ(handler_.record().logLeadershipTermId_rv, INT64_C(0xAAAAAAAAAAAAAAAA));
    EXPECT_EQ(handler_.record().logPosition_rv,         INT64_C(0xBBBBBBBBBBBBBBBB));
    EXPECT_EQ(handler_.record().candidateTermId_rv,     INT64_C(0xCCCCCCCCCCCCCCCC));
    EXPECT_EQ(handler_.record().candidateMemberId_rv,   5);
}

TEST_F(PublisherAdapterFixture, RequestVote_ToMemberPreserved)
{
    publisher_.requestVote(17, 0, 0, 0, 0);
    EXPECT_EQ(sink_.last().toMember, 17);
}

TEST_F(PublisherAdapterFixture, RequestVote_DispatchesOnlyRequestVote)
{
    publisher_.requestVote(1, 0, 0, 0, 0);
    loopback();
    EXPECT_EQ(handler_.calls(), 1);
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::RequestVote);
}

// ---------------------------------------------------------------------------
// 3. placeVote (Vote) round-trip
// ---------------------------------------------------------------------------

TEST_F(PublisherAdapterFixture, PlaceVote_RoundTripTrue)
{
    publisher_.placeVote(
        /*toMember=*/           2,
        /*candidateTermId=*/    INT64_C(0x1234567890ABCDEF),
        /*logLeadershipTermId=*/INT64_C(0xFEDCBA9876543210),
        /*logPosition=*/        INT64_C(0x0102030405060708),
        /*candidateMemberId=*/  8,
        /*followerMemberId=*/   4,
        /*vote=*/               true);

    loopback();

    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::Vote);
    EXPECT_EQ(handler_.record().nowNs_vote,               kNow);
    EXPECT_EQ(handler_.record().candidateTermId_vote,     INT64_C(0x1234567890ABCDEF));
    EXPECT_EQ(handler_.record().logLeadershipTermId_vote, INT64_C(0xFEDCBA9876543210));
    EXPECT_EQ(handler_.record().logPosition_vote,         INT64_C(0x0102030405060708));
    EXPECT_EQ(handler_.record().candidateMemberId_vote,   8);
    EXPECT_EQ(handler_.record().followerMemberId_vote,    4);
    EXPECT_EQ(handler_.record().vote_vote,                true);
}

TEST_F(PublisherAdapterFixture, PlaceVote_RoundTripFalse)
{
    publisher_.placeVote(1, 10, 20, 30, 1, 2, false);
    loopback();
    EXPECT_EQ(handler_.record().vote_vote, false);
}

TEST_F(PublisherAdapterFixture, PlaceVote_ToMemberPreserved)
{
    publisher_.placeVote(55, 0, 0, 0, 0, 0, true);
    EXPECT_EQ(sink_.last().toMember, 55);
}

TEST_F(PublisherAdapterFixture, PlaceVote_DispatchesOnlyVote)
{
    publisher_.placeVote(1, 0, 0, 0, 0, 0, false);
    loopback();
    EXPECT_EQ(handler_.calls(), 1);
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::Vote);
}

// ---------------------------------------------------------------------------
// 4. NewLeadershipTerm round-trip
// ---------------------------------------------------------------------------

TEST_F(PublisherAdapterFixture, NewLeadershipTerm_RoundTrip)
{
    publisher_.newLeadershipTerm(
        /*toMember=*/               11,
        /*logLeadershipTermId=*/    INT64_C(0x0102030405060708),
        /*nextLeadershipTermId=*/   INT64_C(0x1112131415161718),
        /*nextTermBaseLogPosition=*/INT64_C(0x2122232425262728),
        /*nextLogPosition=*/        INT64_C(0x3132333435363738),
        /*leadershipTermId=*/       INT64_C(0x4142434445464748),
        /*termBaseLogPosition=*/    INT64_C(0x5152535455565758),
        /*logPosition=*/            INT64_C(0x6162636465666768),
        /*leaderRecordingId=*/      INT64_C(0x7172737475767778),
        /*timestamp=*/              INT64_C(0x8182838485868788),
        /*leaderMemberId=*/         13,
        /*logSessionId=*/           42,
        /*isStartup=*/              true,
        /*commitPosition=*/         INT64_C(0xC1C2C3C4C5C6C7C8));

    loopback();

    const auto& r = handler_.record();
    EXPECT_EQ(r.which, HandlerRecord::Which::NewLeadershipTerm);
    EXPECT_EQ(r.nowNs_nlt,                   kNow);
    EXPECT_EQ(r.logLeadershipTermId_nlt,     INT64_C(0x0102030405060708));
    EXPECT_EQ(r.nextLeadershipTermId_nlt,    INT64_C(0x1112131415161718));
    EXPECT_EQ(r.nextTermBaseLogPosition_nlt, INT64_C(0x2122232425262728));
    EXPECT_EQ(r.nextLogPosition_nlt,         INT64_C(0x3132333435363738));
    EXPECT_EQ(r.leadershipTermId_nlt,        INT64_C(0x4142434445464748));
    EXPECT_EQ(r.termBaseLogPosition_nlt,     INT64_C(0x5152535455565758));
    EXPECT_EQ(r.logPosition_nlt,             INT64_C(0x6162636465666768));
    EXPECT_EQ(r.leaderRecordingId_nlt,       INT64_C(0x7172737475767778));
    EXPECT_EQ(r.timestamp_nlt,               INT64_C(0x8182838485868788));
    EXPECT_EQ(r.leaderMemberId_nlt,          13);
    EXPECT_EQ(r.logSessionId_nlt,            42);
    EXPECT_EQ(r.isStartup_nlt,               true);
    EXPECT_EQ(r.commitPosition_nlt,          INT64_C(0xC1C2C3C4C5C6C7C8));
}

TEST_F(PublisherAdapterFixture, NewLeadershipTerm_IsStartupFalse)
{
    publisher_.newLeadershipTerm(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, 0);
    loopback();
    EXPECT_EQ(handler_.record().isStartup_nlt, false);
}

TEST_F(PublisherAdapterFixture, NewLeadershipTerm_ToMemberPreserved)
{
    publisher_.newLeadershipTerm(77, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, 0);
    EXPECT_EQ(sink_.last().toMember, 77);
}

TEST_F(PublisherAdapterFixture, NewLeadershipTerm_DispatchesOnlyNLT)
{
    publisher_.newLeadershipTerm(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, 0);
    loopback();
    EXPECT_EQ(handler_.calls(), 1);
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::NewLeadershipTerm);
}

// ---------------------------------------------------------------------------
// 5. AppendPosition round-trip
// ---------------------------------------------------------------------------

TEST_F(PublisherAdapterFixture, AppendPosition_RoundTrip)
{
    publisher_.appendPosition(
        /*toMember=*/       6,
        /*leadershipTermId=*/INT64_C(0xAABBCCDDEEFF0011),
        /*logPosition=*/    INT64_C(0x2233445566778899),
        /*followerMemberId=*/9,
        /*flags=*/          uint8_t{0x01});

    loopback();

    const auto& r = handler_.record();
    EXPECT_EQ(r.which, HandlerRecord::Which::AppendPosition);
    EXPECT_EQ(r.nowNs_ap,             kNow);
    EXPECT_EQ(r.leadershipTermId_ap,  INT64_C(0xAABBCCDDEEFF0011));
    EXPECT_EQ(r.logPosition_ap,       INT64_C(0x2233445566778899));
    EXPECT_EQ(r.followerMemberId_ap,  9);
    EXPECT_EQ(r.flags_ap,             uint8_t{0x01});
}

TEST_F(PublisherAdapterFixture, AppendPosition_FlagsZero)
{
    publisher_.appendPosition(1, 100, 200, 3, uint8_t{0x00});
    loopback();
    EXPECT_EQ(handler_.record().flags_ap, uint8_t{0x00});
}

TEST_F(PublisherAdapterFixture, AppendPosition_ToMemberPreserved)
{
    publisher_.appendPosition(33, 0, 0, 0, 0);
    EXPECT_EQ(sink_.last().toMember, 33);
}

TEST_F(PublisherAdapterFixture, AppendPosition_DispatchesOnlyAppend)
{
    publisher_.appendPosition(1, 0, 0, 0, 0);
    loopback();
    EXPECT_EQ(handler_.calls(), 1);
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::AppendPosition);
}

// ---------------------------------------------------------------------------
// 6. CommitPosition round-trip
// ---------------------------------------------------------------------------

TEST_F(PublisherAdapterFixture, CommitPosition_RoundTrip)
{
    publisher_.commitPosition(
        /*toMember=*/       4,
        /*leadershipTermId=*/INT64_C(0x1234567890ABCDEF),
        /*logPosition=*/    INT64_C(0xFEDCBA0987654321),
        /*leaderMemberId=*/ 12);

    loopback();

    const auto& r = handler_.record();
    EXPECT_EQ(r.which, HandlerRecord::Which::CommitPosition);
    EXPECT_EQ(r.nowNs_cp,            kNow);
    EXPECT_EQ(r.leadershipTermId_cp, INT64_C(0x1234567890ABCDEF));
    EXPECT_EQ(r.logPosition_cp,      INT64_C(0xFEDCBA0987654321));
    EXPECT_EQ(r.leaderMemberId_cp,   12);
}

TEST_F(PublisherAdapterFixture, CommitPosition_ToMemberPreserved)
{
    publisher_.commitPosition(88, 0, 0, 0);
    EXPECT_EQ(sink_.last().toMember, 88);
}

TEST_F(PublisherAdapterFixture, CommitPosition_DispatchesOnlyCommit)
{
    publisher_.commitPosition(1, 0, 0, 0);
    loopback();
    EXPECT_EQ(handler_.calls(), 1);
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::CommitPosition);
}

// ---------------------------------------------------------------------------
// Buffer reuse: calling publisher twice produces correct bytes both times.
// ---------------------------------------------------------------------------

TEST_F(PublisherAdapterFixture, BufferReuse_CanvassThenCommit)
{
    // First: canvass
    publisher_.canvassPosition(1,
        INT64_C(0xAAAA), INT64_C(0xBBBB), INT64_C(0xCCCC), 5);
    loopback();
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::Canvass);
    EXPECT_EQ(handler_.record().logLeadershipTermId_canvass, INT64_C(0xAAAA));

    handler_.reset();

    // Second (different message type): commitPosition — buffer reused
    publisher_.commitPosition(2,
        INT64_C(0x9999), INT64_C(0x8888), 7);
    loopback();
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::CommitPosition);
    EXPECT_EQ(handler_.record().leadershipTermId_cp, INT64_C(0x9999));
    EXPECT_EQ(handler_.record().logPosition_cp,      INT64_C(0x8888));
    EXPECT_EQ(handler_.record().leaderMemberId_cp,   7);
}

TEST_F(PublisherAdapterFixture, BufferReuse_SameMessageTwice)
{
    // First call
    publisher_.appendPosition(1, INT64_C(0x1111), INT64_C(0x2222), 3, uint8_t{0xAB});
    std::vector<uint8_t> firstBytes = sink_.last().bytes;

    // Second call with different values
    publisher_.appendPosition(2, INT64_C(0x3333), INT64_C(0x4444), 5, uint8_t{0xCD});
    std::vector<uint8_t> secondBytes = sink_.last().bytes;

    // Bytes differ (not stale)
    EXPECT_NE(firstBytes, secondBytes);

    // Second loopback decodes correctly
    adapter_.onFragment(kNow, secondBytes.data(), secondBytes.size());
    EXPECT_EQ(handler_.record().leadershipTermId_ap, INT64_C(0x3333));
    EXPECT_EQ(handler_.record().logPosition_ap,      INT64_C(0x4444));
    EXPECT_EQ(handler_.record().followerMemberId_ap, 5);
    EXPECT_EQ(handler_.record().flags_ap,            uint8_t{0xCD});
}

// ---------------------------------------------------------------------------
// peekTemplateId dispatch: each message routes to exactly its handler.
// ---------------------------------------------------------------------------

TEST_F(PublisherAdapterFixture, Dispatch_CanvassNotCallsOthers)
{
    publisher_.canvassPosition(1, 0, 0, 0, 0);
    loopback();
    EXPECT_EQ(handler_.calls(), 1);
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::Canvass);
}

TEST_F(PublisherAdapterFixture, Dispatch_AllSixDistinctMessages)
{
    // Publish one of each and verify each dispatches to exactly the right method.
    publisher_.canvassPosition(1, 10, 20, 30, 1);
    loopback();
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::Canvass);
    handler_.reset();

    publisher_.requestVote(1, 10, 20, 30, 1);
    loopback();
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::RequestVote);
    handler_.reset();

    publisher_.placeVote(1, 10, 20, 30, 1, 2, true);
    loopback();
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::Vote);
    handler_.reset();

    publisher_.newLeadershipTerm(1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 1, 2, false, 100);
    loopback();
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::NewLeadershipTerm);
    handler_.reset();

    publisher_.appendPosition(1, 10, 20, 1, uint8_t{0x01});
    loopback();
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::AppendPosition);
    handler_.reset();

    publisher_.commitPosition(1, 10, 20, 1);
    loopback();
    EXPECT_EQ(handler_.record().which, HandlerRecord::Which::CommitPosition);
}

// ---------------------------------------------------------------------------
// FRAGMENT_LIMIT constant mirrors Java
// ---------------------------------------------------------------------------

TEST(ConsensusAdapterConstants, FragmentLimitIs10)
{
    EXPECT_EQ(ConsensusAdapter::FRAGMENT_LIMIT, 10);
}

// ---------------------------------------------------------------------------
// Part B-3 (covers A3): Truncated / oversized SBE frame into onFragment.
//
// Without the A3 fix, 'len' was ignored (/*len*/ comment) so a frame shorter
// than TOTAL_SIZE caused an OOB read / UB when decode() accessed fixed offsets
// past the end of the buffer.  With the fix every decode path is guarded by
// len >= Msg::TOTAL_SIZE.
//
// These tests verify:
//   a) A frame shorter than ConsensusMessageHeader::ENCODED_SIZE is rejected
//      without dispatching to any handler.
//   b) A frame shorter than Msg::TOTAL_SIZE (but >= header) is rejected.
//   c) An oversized frame (extra trailing bytes beyond TOTAL_SIZE) is accepted
//      and decoded correctly (trailing bytes are ignored, as in Aeron).
//   d) ignoredFrameCount() is incremented on each rejected frame.
// ---------------------------------------------------------------------------

TEST(ConsensusAdapterBounds, PartB3_TooShortForHeader_Rejected) {
    RecordingHandler handler;
    ConsensusAdapter adapter{handler};

    // 3 bytes — shorter than the 8-byte SBE header.
    std::array<uint8_t, 3> buf{};
    adapter.onFragment(0, buf.data(), buf.size());

    EXPECT_EQ(handler.calls(), 0)
        << "A3/B-3: frame shorter than header must not dispatch to any handler";
    EXPECT_EQ(adapter.ignoredFrameCount(), 1)
        << "A3/B-3: ignoredFrameCount must increment on header-too-short rejection";
}

TEST(ConsensusAdapterBounds, PartB3_TruncatedAppendPosition_Rejected) {
    RecordingHandler handler;
    ConsensusAdapter adapter{handler};

    // Build a full-size AppendPosition then truncate by 5 bytes.
    std::array<uint8_t, AppendPositionMsg::TOTAL_SIZE> fullBuf{};
    AppendPositionMsg::encode(fullBuf.data(), 0,
        /*leadershipTermId=*/INT64_C(0x1234),
        /*logPosition=*/     INT64_C(0x5678),
        /*followerMemberId=*/1,
        /*flags=*/           uint8_t{0x00});

    const std::size_t truncLen = AppendPositionMsg::TOTAL_SIZE - 5;
    adapter.onFragment(0, fullBuf.data(), truncLen);

    EXPECT_EQ(handler.calls(), 0)
        << "A3/B-3: truncated AppendPosition must not dispatch (OOB read guard)";
    EXPECT_EQ(adapter.ignoredFrameCount(), 1)
        << "A3/B-3: ignoredFrameCount must increment on truncated-body rejection";
}

TEST(ConsensusAdapterBounds, PartB3_TruncatedNLT_Rejected) {
    RecordingHandler handler;
    ConsensusAdapter adapter{handler};

    // Build a valid NLT then truncate to header-only (no payload at all).
    std::array<uint8_t, NewLeadershipTermMsg::TOTAL_SIZE> fullBuf{};
    NewLeadershipTermMsg::encode(fullBuf.data(), 0,
        /*logLeadershipTermId=*/   0,
        /*nextLeadershipTermId=*/  0,
        /*nextTermBaseLogPos=*/    0,
        /*nextLogPosition=*/       0,
        /*leadershipTermId=*/      0,
        /*termBaseLogPosition=*/   0,
        /*logPosition=*/           0,
        /*leaderRecordingId=*/     0,
        /*timestamp=*/             0,
        /*leaderMemberId=*/        0,
        /*logSessionId=*/          0,
        /*appVersion=*/            0,
        /*isStartup=*/             BooleanType::FALSE_VALUE,
        /*commitPosition=*/        0);

    // len = header size only (payload missing).
    adapter.onFragment(0, fullBuf.data(), ConsensusMessageHeader::ENCODED_SIZE);

    EXPECT_EQ(handler.calls(), 0)
        << "A3/B-3: NLT truncated to header-only must not dispatch";
    EXPECT_EQ(adapter.ignoredFrameCount(), 1);
}

TEST(ConsensusAdapterBounds, PartB3_OversizedFrame_DecodedCorrectly) {
    // An oversized frame (extra trailing bytes beyond TOTAL_SIZE) must be accepted
    // and the known fields decoded correctly — Aeron ignores trailing bytes.
    RecordingHandler handler;
    ConsensusAdapter adapter{handler};

    // Allocate a buffer larger than TOTAL_SIZE by 32 bytes (simulates future schema fields).
    constexpr std::size_t kExtra = 32;
    std::array<uint8_t, CommitPositionMsg::TOTAL_SIZE + kExtra> bigBuf{};
    CommitPositionMsg::encode(bigBuf.data(), 0,
        /*leadershipTermId=*/INT64_C(0xABCDEF),
        /*logPosition=*/     INT64_C(0x123456),
        /*leaderMemberId=*/  7);

    // Pass the full oversized length.
    adapter.onFragment(0, bigBuf.data(), bigBuf.size());

    EXPECT_EQ(handler.calls(), 1)
        << "A3/B-3: oversized frame must be dispatched (trailing bytes ignored)";
    EXPECT_EQ(handler.record().which,             HandlerRecord::Which::CommitPosition);
    EXPECT_EQ(handler.record().leadershipTermId_cp, INT64_C(0xABCDEF));
    EXPECT_EQ(handler.record().logPosition_cp,      INT64_C(0x123456));
    EXPECT_EQ(handler.record().leaderMemberId_cp,   7);
    EXPECT_EQ(adapter.ignoredFrameCount(), 0)
        << "A3/B-3: oversized frame must NOT increment ignoredFrameCount";
}

TEST(ConsensusAdapterBounds, PartB3_MultipleShortFrames_IgnoredCountAccumulates) {
    RecordingHandler handler;
    ConsensusAdapter adapter{handler};

    // Inject 3 short frames in sequence — count must accumulate.
    std::array<uint8_t, 4> tinyBuf{};
    adapter.onFragment(0, tinyBuf.data(), 4);
    adapter.onFragment(0, tinyBuf.data(), 0);
    adapter.onFragment(0, tinyBuf.data(), 2);

    EXPECT_EQ(handler.calls(), 0);
    EXPECT_EQ(adapter.ignoredFrameCount(), 3)
        << "A3/B-3: ignoredFrameCount must accumulate across multiple rejected frames";
}

TEST(ConsensusAdapterBounds, PartB3_TruncatedVote_Rejected) {
    RecordingHandler handler;
    ConsensusAdapter adapter{handler};

    // VoteMsg: truncate to exactly 1 byte short of TOTAL_SIZE.
    std::array<uint8_t, VoteMsg::TOTAL_SIZE> fullBuf{};
    VoteMsg::encode(fullBuf.data(), 0,
        /*candidateTermId=*/    1,
        /*logLeadershipTermId=*/2,
        /*logPosition=*/        3,
        /*candidateMemberId=*/  4,
        /*followerMemberId=*/   5,
        /*vote=*/               BooleanType::TRUE_VALUE);

    adapter.onFragment(0, fullBuf.data(), VoteMsg::TOTAL_SIZE - 1);

    EXPECT_EQ(handler.calls(), 0)
        << "A3/B-3: truncated Vote must not dispatch";
    EXPECT_EQ(adapter.ignoredFrameCount(), 1);
}

TEST(ConsensusAdapterBounds, PartB3_ExactSizeFrame_Dispatched) {
    // Verify that a frame at exactly TOTAL_SIZE passes the guard (boundary test).
    RecordingHandler handler;
    ConsensusAdapter adapter{handler};

    std::array<uint8_t, RequestVoteMsg::TOTAL_SIZE> buf{};
    RequestVoteMsg::encode(buf.data(), 0,
        /*logLeadershipTermId=*/INT64_C(0x11),
        /*logPosition=*/        INT64_C(0x22),
        /*candidateTermId=*/    INT64_C(0x33),
        /*candidateMemberId=*/  5,
        /*protocolVersion=*/    17);

    adapter.onFragment(0, buf.data(), buf.size());

    EXPECT_EQ(handler.calls(), 1)
        << "A3/B-3: frame at exactly TOTAL_SIZE must be dispatched (>= boundary)";
    EXPECT_EQ(handler.record().which, HandlerRecord::Which::RequestVote);
    EXPECT_EQ(handler.record().logLeadershipTermId_rv, INT64_C(0x11));
    EXPECT_EQ(adapter.ignoredFrameCount(), 0);
}

// ---------------------------------------------------------------------------
// Unknown templateId silently ignored (forward-compat)
// ---------------------------------------------------------------------------

TEST(ConsensusAdapterDispatch, UnknownTemplateIdIgnored)
{
    RecordingHandler handler;
    ConsensusAdapter adapter{handler};

    // Craft a valid SBE header with an unknown templateId (e.g., 99).
    std::array<uint8_t, 8> buf{};
    ConsensusMessageHeader::encode(buf.data(), 0, 0, /*templateId=*/99);

    adapter.onFragment(0, buf.data(), buf.size());

    EXPECT_EQ(handler.calls(), 0); // nothing dispatched
}

// D29: PreVoteRequest (60) / PreVoteResponse (61) / TermFloorNotice (62) through
// the production SBE publisher and adapter — a missing override anywhere
// (publisher, adapter, handler) would silently disable PreVote.
namespace {
struct D29Handler {
    int req = 0, resp = 0, notice = 0;
    Term term = 0, llt = 0, floor = 0, refused = 0, voterTerm = 0; int64_t pos = 0; NodeId who = -1; bool granted = false;
    void onCanvassPosition(int64_t, Term, int64_t, Term, NodeId, int32_t) {}
    void onRequestVote(int64_t, Term, int64_t, Term, NodeId, int32_t) {}
    void onVote(int64_t, Term, Term, int64_t, NodeId, NodeId, bool) {}
    void onNewLeadershipTerm(int64_t, Term, Term, int64_t, int64_t, Term, int64_t, int64_t, int64_t,
                             int64_t, NodeId, int32_t, int32_t, bool, int64_t) {}
    void onAppendPosition(int64_t, Term, int64_t, NodeId, uint8_t) {}
    void onCommitPosition(int64_t, Term, int64_t, NodeId) {}
    void onPreVoteRequest(int64_t, Term t, Term l, int64_t p, NodeId c) { ++req; term = t; llt = l; pos = p; who = c; }
    void onPreVoteResponse(int64_t, Term t, NodeId v, bool g, Term vt) { ++resp; term = t; who = v; granted = g; voterTerm = vt; }
    void onTermFloorNotice(int64_t, NodeId f, Term fl, Term r) { ++notice; who = f; floor = fl; refused = r; }
};
}
TEST(ConsensusMessagesRegression, PreVoteAndFloorNoticeRoundTripThroughPublisherAndAdapterRegression) {
    LoopbackSink sink;
    SbeConsensusPublisher pub(sink);
    D29Handler h;
    BasicConsensusAdapter<D29Handler> adapter{h};
    pub.preVoteRequest(/*to=*/2, /*term=*/9, /*llt=*/7, /*pos=*/123456, /*candidate=*/1);
    EXPECT_EQ(sink.last().toMember, 2);
    adapter.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size());
    ASSERT_EQ(h.req, 1);
    EXPECT_EQ(h.term, 9); EXPECT_EQ(h.llt, 7); EXPECT_EQ(h.pos, 123456); EXPECT_EQ(h.who, 1);
    pub.preVoteResponse(/*to=*/1, /*term=*/9, /*voter=*/2, /*granted=*/true, /*voterTerm=*/11);
    adapter.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size());
    ASSERT_EQ(h.resp, 1);
    EXPECT_EQ(h.term, 9); EXPECT_EQ(h.who, 2); EXPECT_TRUE(h.granted); EXPECT_EQ(h.voterTerm, 11);
    pub.preVoteResponse(1, 9, 2, false, 12);
    adapter.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size());
    EXPECT_FALSE(h.granted);
    pub.termFloorNotice(/*to=*/0, /*from=*/3, /*floor=*/12, /*refused=*/10);
    adapter.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size());
    ASSERT_EQ(h.notice, 1);
    EXPECT_EQ(h.who, 3); EXPECT_EQ(h.floor, 12); EXPECT_EQ(h.refused, 10);
    adapter.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size() - 1);   // truncated: ignored
    EXPECT_EQ(h.notice, 1);
}

// Issue #82 (VLS A.4): ReadLeaseRequest (63) / ReadLeaseGrant (64) through the production SBE publisher and adapter - a
// missing override anywhere (publisher, adapter, agent) would leave every view read answering NotLeader.  Truncated
// frames are ignored; an adapter whose handler has no lease handlers (an older broker) ignores both.
namespace {
struct LeaseHandler {
    int req = 0, grant = 0;
    Term term = 0; NodeId member = -1, leader = -1; int64_t reqSeq = 0, heard = 0, grantSeq = 0, commitPos = 0, validFor = 0;
    void onCanvassPosition(int64_t, Term, int64_t, Term, NodeId, int32_t) {}
    void onRequestVote(int64_t, Term, int64_t, Term, NodeId, int32_t) {}
    void onVote(int64_t, Term, Term, int64_t, NodeId, NodeId, bool) {}
    void onNewLeadershipTerm(int64_t, Term, Term, int64_t, int64_t, Term, int64_t, int64_t, int64_t,
                             int64_t, NodeId, int32_t, int32_t, bool, int64_t) {}
    void onAppendPosition(int64_t, Term, int64_t, NodeId, uint8_t) {}
    void onCommitPosition(int64_t, Term, int64_t, NodeId) {}
    void onReadLeaseRequest(int64_t, Term t, NodeId m, int64_t rs, int64_t h) { ++req; term = t; member = m; reqSeq = rs; heard = h; }
    void onReadLeaseGrant(int64_t, Term t, NodeId l, NodeId m, int64_t rs, int64_t gs, int64_t cp, int64_t v) {
        ++grant; term = t; leader = l; member = m; reqSeq = rs; grantSeq = gs; commitPos = cp; validFor = v;
    }
};
struct OldHandler {   // a pre-#82 broker: no lease handlers
    int calls = 0;
    void onCanvassPosition(int64_t, Term, int64_t, Term, NodeId, int32_t) { ++calls; }
    void onRequestVote(int64_t, Term, int64_t, Term, NodeId, int32_t) { ++calls; }
    void onVote(int64_t, Term, Term, int64_t, NodeId, NodeId, bool) { ++calls; }
    void onNewLeadershipTerm(int64_t, Term, Term, int64_t, int64_t, Term, int64_t, int64_t, int64_t,
                             int64_t, NodeId, int32_t, int32_t, bool, int64_t) { ++calls; }
    void onAppendPosition(int64_t, Term, int64_t, NodeId, uint8_t) { ++calls; }
    void onCommitPosition(int64_t, Term, int64_t, NodeId) { ++calls; }
};
}
TEST(ConsensusMessagesRegression, ReadLeaseRequestAndGrantRoundTripThroughPublisherAndAdapterRegression) {
    LoopbackSink sink;
    SbeConsensusPublisher pub(sink);
    LeaseHandler h;
    BasicConsensusAdapter<LeaseHandler> adapter{h};
    pub.readLeaseRequest(/*to=*/1, /*term=*/9, /*member=*/2, /*reqSeq=*/77, /*heard=*/66);
    EXPECT_EQ(sink.last().toMember, 1);
    EXPECT_EQ(sink.last().bytes.size(), ReadLeaseRequestMsg::TOTAL_SIZE);
    adapter.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size());
    ASSERT_EQ(h.req, 1);
    EXPECT_EQ(h.term, 9); EXPECT_EQ(h.member, 2); EXPECT_EQ(h.reqSeq, 77); EXPECT_EQ(h.heard, 66);
    adapter.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size() - 1);   // truncated: ignored
    EXPECT_EQ(h.req, 1);
    pub.readLeaseGrant(/*to=*/2, /*term=*/9, /*leader=*/1, /*member=*/2, /*reqSeq=*/77, /*grantSeq=*/88,
                       /*commitPos=*/123456789, /*validFor=*/111'222'333);
    EXPECT_EQ(sink.last().toMember, 2);
    EXPECT_EQ(sink.last().bytes.size(), ReadLeaseGrantMsg::TOTAL_SIZE);
    adapter.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size());
    ASSERT_EQ(h.grant, 1);
    EXPECT_EQ(h.term, 9); EXPECT_EQ(h.leader, 1); EXPECT_EQ(h.member, 2); EXPECT_EQ(h.reqSeq, 77);
    EXPECT_EQ(h.grantSeq, 88); EXPECT_EQ(h.commitPos, 123456789); EXPECT_EQ(h.validFor, 111'222'333);
    adapter.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size() - 1);
    EXPECT_EQ(h.grant, 1);
    OldHandler o;
    BasicConsensusAdapter<OldHandler> old{o};
    old.onFragment(0, sink.last().bytes.data(), sink.last().bytes.size());
    EXPECT_EQ(o.calls, 0) << "an older broker ignores the lease templates";
    EXPECT_EQ(peekTemplateId(sink.last().bytes.data()), 64);
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::ReadLeaseRequest), 63);
    EXPECT_EQ(static_cast<uint16_t>(ConsensusTemplateId::ReadLeaseGrant), 64);
}

// PR #490 adversarial review: both lease templates truncated at EVERY byte offset are ignored (counted, never handed to
// the handler, never read past the end).
TEST(ConsensusMessagesRegression, ReadLeaseTemplatesTruncatedAtEveryOffsetAreIgnoredAdversarialRegression) {
    LoopbackSink sink;
    SbeConsensusPublisher pub(sink);
    pub.readLeaseRequest(1, 9, 2, 77, 66);
    const std::vector<uint8_t> req = sink.last().bytes;
    pub.readLeaseGrant(2, 9, 1, 2, 77, 88, 123, 456);
    const std::vector<uint8_t> grant = sink.last().bytes;
    for (const auto* frame : {&req, &grant}) {
        for (std::size_t len = 0; len < frame->size(); ++len) {
            LeaseHandler h;
            BasicConsensusAdapter<LeaseHandler> adapter{h};
            std::vector<uint8_t> cut(frame->begin(), frame->begin() + static_cast<std::ptrdiff_t>(len));
            adapter.onFragment(0, cut.data(), cut.size());
            EXPECT_EQ(h.req + h.grant, 0) << "template " << (frame == &req ? 63 : 64) << " truncated to " << len << " B";
        }
        LeaseHandler h;
        BasicConsensusAdapter<LeaseHandler> adapter{h};
        std::vector<uint8_t> longer = *frame;
        longer.push_back(0xAB);   // trailing byte: still one message
        adapter.onFragment(0, longer.data(), longer.size());
        EXPECT_EQ(h.req + h.grant, 1);
    }
}
