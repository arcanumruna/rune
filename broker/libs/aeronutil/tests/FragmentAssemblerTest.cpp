// FragmentAssemblerTest.cpp — Task 11 fix 2 (B3): rune::aeronutil's templated
// FragmentAssembler / ControlledFragmentAssembler reassemble exactly like
// Aeron's (single, multi-fragment, interleaved sessions, gap reset, ABORT).
// Frames are laid out as in a real term buffer: 32-byte data header, frames
// 32-byte aligned; the Header object points at each frame like Image::poll does.
#include <rune/aeronutil/FragmentAssembler.h>

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using rune::aeronutil::ControlledFragmentAssembler;
using rune::aeronutil::FragmentAssembler;

namespace {
struct Term {
    std::vector<std::uint8_t> bytes = std::vector<std::uint8_t>(64 * 1024, 0);
    AtomicBuffer buf{bytes.data(), bytes.size()};
    aeron::util::index_t tail = 0;
    struct Frame { aeron::util::index_t frameOffset, payloadOffset, length; };
    Frame put(std::int32_t sessionId, std::uint8_t flags, const std::string& payload) {
        Frame f{tail, tail + DataFrameHeader::LENGTH, static_cast<aeron::util::index_t>(payload.size())};
        buf.putInt32(f.frameOffset, DataFrameHeader::LENGTH + f.length);
        buf.putUInt8(f.frameOffset + DataFrameHeader::FLAGS_FIELD_OFFSET, flags);
        buf.putInt32(f.frameOffset + DataFrameHeader::SESSION_ID_FIELD_OFFSET, sessionId);
        std::memcpy(bytes.data() + f.payloadOffset, payload.data(), payload.size());
        tail = aeron::util::BitUtil::align(f.payloadOffset + f.length, FrameDescriptor::FRAME_ALIGNMENT);
        return f;
    }
};
constexpr std::uint8_t B = FrameDescriptor::BEGIN_FRAG, E = FrameDescriptor::END_FRAG, U = B | E, M = 0;

struct Sink {
    std::vector<std::string> msgs;
    std::vector<const std::uint8_t*> ptrs;
};

template <class Asm>
void feed(Asm& a, Term& t, const Term::Frame& f, Sink& s) {
    Header h(0, static_cast<aeron::util::index_t>(t.bytes.size()), nullptr);
    h.buffer(t.buf);
    h.offset(f.frameOffset);
    a.onFragment([&s](AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, Header&) {
        s.msgs.emplace_back(reinterpret_cast<const char*>(b.buffer() + o), static_cast<std::size_t>(l));
        s.ptrs.push_back(b.buffer() + o);
    }, t.buf, f.payloadOffset, f.length, h);
}
} // namespace

TEST(FragmentAssemblerRegression, UnfragmentedIsDeliveredInPlace) {
    Term t; Sink s; FragmentAssembler a;
    const auto f = t.put(7, U, "hello");
    feed(a, t, f, s);
    ASSERT_EQ(s.msgs, std::vector<std::string>{"hello"});
    EXPECT_EQ(s.ptrs[0], t.bytes.data() + f.payloadOffset) << "zero copy for whole messages";
    EXPECT_EQ(a.sessionCountForTest(), 0u) << "the hot path never touches the session table";
}

TEST(FragmentAssemblerRegression, MultiFragmentMessageIsReassembled) {
    Term t; Sink s; FragmentAssembler a;
    const auto f1 = t.put(7, B, "abc"), f2 = t.put(7, M, "defgh"), f3 = t.put(7, E, "ij");
    feed(a, t, f1, s); feed(a, t, f2, s);
    EXPECT_TRUE(s.msgs.empty()) << "nothing before END";
    feed(a, t, f3, s);
    EXPECT_EQ(s.msgs, std::vector<std::string>{"abcdefghij"});
}

TEST(FragmentAssemblerRegression, InterleavedSessionsReassembleIndependently) {
    // Each session has its own term in a real image; interleave the deliveries.
    Term ta, tb; Sink s; FragmentAssembler a;
    const auto a1 = ta.put(1, B, "A1-"), a2 = ta.put(1, E, "A2");
    const auto b1 = tb.put(2, B, "B1-"), b2 = tb.put(2, E, "B2");
    feed(a, ta, a1, s); feed(a, tb, b1, s); feed(a, ta, a2, s); feed(a, tb, b2, s);
    EXPECT_EQ(s.msgs, (std::vector<std::string>{"A1-A2", "B1-B2"}));
    EXPECT_EQ(a.sessionCountForTest(), 2u);
}

TEST(FragmentAssemblerRegression, GapResetsThePartialMessage) {
    Term t; Sink s; FragmentAssembler a;
    const auto f1 = t.put(7, B, "abc");
    (void)t.put(7, M, "LOST");                  // never delivered: a gap
    const auto f3 = t.put(7, E, "xyz");
    feed(a, t, f1, s); feed(a, t, f3, s);
    EXPECT_TRUE(s.msgs.empty()) << "a non-contiguous fragment must drop the partial message";
    const auto g1 = t.put(7, B, "ok-"), g2 = t.put(7, E, "fine");
    feed(a, t, g1, s); feed(a, t, g2, s);
    EXPECT_EQ(s.msgs, std::vector<std::string>{"ok-fine"}) << "the next whole message still assembles";
}

TEST(FragmentAssemblerRegression, ControlledAbortRedeliversTheEndFragment) {
    Term t; ControlledFragmentAssembler a;
    const auto f1 = t.put(9, B, "abc"), f2 = t.put(9, E, "def");
    std::vector<std::string> got;
    int calls = 0;
    auto deliver = [&](const Term::Frame& f) {
        Header h(0, static_cast<aeron::util::index_t>(t.bytes.size()), nullptr);
        h.buffer(t.buf); h.offset(f.frameOffset);
        return a.onFragment([&](AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, Header&) {
            ++calls;
            if (calls == 1) return aeron::ControlledPollAction::ABORT;   // back-pressured consumer
            got.emplace_back(reinterpret_cast<const char*>(b.buffer() + o), static_cast<std::size_t>(l));
            return aeron::ControlledPollAction::CONTINUE;
        }, t.buf, f.payloadOffset, f.length, h);
    };
    EXPECT_EQ(deliver(f1), aeron::ControlledPollAction::CONTINUE);
    EXPECT_EQ(deliver(f2), aeron::ControlledPollAction::ABORT);
    EXPECT_EQ(deliver(f2), aeron::ControlledPollAction::CONTINUE) << "the image re-delivers END after ABORT";
    EXPECT_EQ(got, std::vector<std::string>{"abcdef"});
}

// Review m-c: per-session buffers of images that went away are freed (on the
// polling thread), so the table cannot grow without bound.
namespace {
struct FakeSub {
    std::vector<std::int32_t> live;
    int imageCount() const { return static_cast<int>(live.size()); }
    std::shared_ptr<int> imageBySessionId(std::int32_t id) const {
        for (auto s : live) if (s == id) return std::make_shared<int>(id);
        return nullptr;
    }
};
}

TEST(FragmentAssemblerRegression, SessionBuffersAreFreedWhenTheirImagesGoAway) {
    constexpr int kSessions = 32;
    Term t; Sink s; FragmentAssembler a; FakeSub sub; rune::aeronutil::ImageLossWatch watch;
    for (int id = 1; id <= kSessions; ++id) {
        sub.live.push_back(id);
        feed(a, t, t.put(id, B, "partial-"), s);          // each session leaves a partial message
        watch.afterPoll(sub, a);
    }
    EXPECT_EQ(a.sessionCountForTest(), static_cast<std::size_t>(kSessions));
    sub.live.clear();                                       // every image goes away
    watch.afterPoll(sub, a);
    EXPECT_EQ(a.sessionCountForTest(), 0u) << "per-session buffers of lost images must be freed";
}

TEST(FragmentAssemblerRegression, ChurnAtConstantImageCountStaysBounded) {
    // One image leaves and another arrives between polls: the count never changes,
    // but the new session's fragment grows the table → purge.
    Term t; Sink s; FragmentAssembler a; FakeSub sub; rune::aeronutil::ImageLossWatch watch;
    sub.live = {1};
    feed(a, t, t.put(1, B, "x"), s); watch.afterPoll(sub, a);
    for (int id = 2; id <= 100; ++id) {
        sub.live = {id};
        feed(a, t, t.put(id, B, "y"), s);
        watch.afterPoll(sub, a);
        ASSERT_LE(a.sessionCountForTest(), 1u) << "table grew past the live images at id " << id;
    }
}
