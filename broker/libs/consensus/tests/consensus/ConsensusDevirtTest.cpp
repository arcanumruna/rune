// ConsensusDevirtTest.cpp — Task 11 fix 2 (B2): the consensus receive path is
// templated on the CONCRETE handler (no ConsensusMessageHandler vtable needed).
// RED-by-construction: the handler below does NOT derive from the interface, so
// it only compiles against the templated adapter.
#include <rune/consensus/ConsensusAdapter.h>
#include <rune/consensus/ConsensusMessages.h>

#include <gtest/gtest.h>

#include <array>
#include <type_traits>

using namespace rune::consensus;

namespace {
struct PlainHandler final {   // NOT a ConsensusMessageHandler
    int commits = 0;
    int64_t lastPos = -1;
    void onCanvassPosition(int64_t, Term, int64_t, Term, NodeId, int32_t) {}
    void onRequestVote(int64_t, Term, int64_t, Term, NodeId, int32_t) {}
    void onVote(int64_t, Term, Term, int64_t, NodeId, NodeId, bool) {}
    void onNewLeadershipTerm(int64_t, Term, Term, int64_t, int64_t, Term, int64_t, int64_t, int64_t,
                             int64_t, NodeId, int32_t, int32_t, bool, int64_t) {}
    void onAppendPosition(int64_t, Term, int64_t, NodeId, uint8_t) {}
    void onCommitPosition(int64_t, Term, int64_t pos, NodeId) { ++commits; lastPos = pos; }
};
} // namespace

TEST(ConsensusAdapterRegression, DispatchesToAConcreteHandlerWithoutTheInterface) {
    static_assert(!std::is_base_of_v<ConsensusMessageHandler, PlainHandler>);
    PlainHandler h;
    BasicConsensusAdapter<PlainHandler> adapter{h};
    std::array<uint8_t, 128> buf{};
    const std::size_t len = CommitPositionMsg::encode(buf.data(), 0, /*leadershipTermId=*/3,
                                                      /*logPosition=*/4096, /*leaderMemberId=*/1);
    adapter.onFragment(0, buf.data(), len);
    EXPECT_EQ(h.commits, 1);
    EXPECT_EQ(h.lastPos, 4096);
    EXPECT_EQ(adapter.ignoredFrameCount(), 0);
}

// ---------------------------------------------------------------------------
// Send path: Election → SbeConsensusPublisher → final sink, all direct calls.
// ---------------------------------------------------------------------------
#include <rune/consensus/NullSink.h>
#include <rune/consensus/SbeConsensusPublisher.h>

namespace {
struct RecordingSink final : MessageSink {   // a test double = Generic arm
    int sends = 0;
    void send(NodeId, const uint8_t*, std::size_t) override { ++sends; }
};
} // namespace

TEST(ConsensusPublisherRegression, ProductionSinksAreDispatchedConcretely) {
    NullSink null;
    SbeConsensusPublisher pubNull(null);
    EXPECT_TRUE(pubNull.sinkIsConcrete()) << "NullSink must be a direct (final) call";
    RecordingSink rec;
    SbeConsensusPublisher pubRec(rec);
    EXPECT_FALSE(pubRec.sinkIsConcrete());
    pubRec.commitPosition(2, 1, 64, 0);
    EXPECT_EQ(rec.sends, 1) << "test doubles still receive sends (Generic arm)";
}
